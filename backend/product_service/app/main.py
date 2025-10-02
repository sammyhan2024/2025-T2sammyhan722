import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional

import aio_pika
from azure.core.exceptions import AzureError
from azure.storage.blob import (
    BlobSasPermissions,
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
)
from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Response,
    UploadFile,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from .db import Base, engine, get_db
from .models import Product
from .schemas import ProductCreate, ProductResponse, ProductUpdate, StockDeductRequest

# --- Standard Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)

# --- Environment and Global Variables ---
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_STORAGE_CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "product-images")
AZURE_SAS_TOKEN_EXPIRY_HOURS = int(os.getenv("AZURE_SAS_TOKEN_EXPIRY_HOURS", "24"))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

RESTOCK_THRESHOLD = 5

# --- Global Clients and Connections ---
blob_service_client: Optional[BlobServiceClient] = None
rabbitmq_connection: Optional[aio_pika.Connection] = None
stock_update_channel: Optional[aio_pika.Channel] = None

# --- FastAPI Application Setup ---
app = FastAPI(
    title="Product Service API",
    description="Manages products and stock for a mini-ecommerce app, with Azure Storage integration.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Service Initialization and Lifecycle Events ---

def initialize_blob_storage():
    """Initializes the Azure Blob Storage client and handles potential errors."""
    global blob_service_client
    if AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY:
        try:
            # Basic validation to prevent the common "Incorrect padding" error
            if len(AZURE_STORAGE_ACCOUNT_KEY) % 4 != 0:
                raise ValueError("The Azure Storage Account Key has an invalid length or format.")
            
            account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)
            
            logger.info("Product Service: Azure BlobServiceClient initialized.")
            
            # Ensure the container exists
            container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER_NAME)
            container_client.create_container()
            logger.info(f"Product Service: Azure container '{AZURE_STORAGE_CONTAINER_NAME}' is ready.")
            
        except (ValueError, AzureError) as e:
            logger.critical(f"Product Service: Failed to initialize Azure BlobServiceClient. Error: {e}", exc_info=True)
            blob_service_client = None
        except Exception as e:
            logger.critical(f"Product Service: An unexpected error occurred during Azure BlobServiceClient initialization. Error: {e}", exc_info=True)
            blob_service_client = None
    else:
        logger.warning("Product Service: Azure Storage credentials not found. Image upload functionality will be disabled.")
        blob_service_client = None

async def connect_to_rabbitmq(max_retries=5, initial_delay=5, max_delay=30):
    """Establishes a connection to RabbitMQ with exponential backoff, returning None on failure."""
    retries = 0
    delay = initial_delay
    while retries < max_retries:
        try:
            connection = await aio_pika.connect_robust(
                host=RABBITMQ_HOST, port=RABBITMQ_PORT, login=RABBITMQ_USER, password=RABBITMQ_PASS, timeout=10
            )
            logger.info("Product Service: Successfully connected to RabbitMQ.")
            return connection
        except Exception as e:
            retries += 1
            logger.warning(f"Product Service: RabbitMQ connection failed (attempt {retries}/{max_retries}). Error: {e}. Retrying in {delay} seconds...")
            if retries >= max_retries:
                break
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)
    
    logger.error("Product Service: Could not connect to RabbitMQ after multiple retries. Messaging features will be disabled.")
    return None

@app.on_event("startup")
async def startup_event():
    """Handles application startup logic: DB, Blob Storage, and RabbitMQ connections."""
    global rabbitmq_connection, stock_update_channel
    logger.info("Product Service: Application startup sequence initiated.")

    # 1. Initialize Azure Blob Storage
    initialize_blob_storage()

    # 2. Establish database connection
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Product Service: Database tables verified/created.")
    except OperationalError as e:
        logger.critical(f"Product Service: Database connection failed: {e}", exc_info=True)
        sys.exit(f"Critical Error: Could not connect to the database. Shutting down. {e}")

    # 3. Establish RabbitMQ connection (optional)
    rabbitmq_connection = await connect_to_rabbitmq()
    if rabbitmq_connection:
        try:
            stock_update_channel = await rabbitmq_connection.channel()
            await stock_update_channel.exchange_declare("product_events", aio_pika.ExchangeType.TOPIC, durable=True)
            logger.info("Product Service: RabbitMQ channel and exchange are ready.")
        except Exception as e:
            logger.error(f"Product Service: Failed to set up RabbitMQ channel/exchange. Error: {e}", exc_info=True)
            stock_update_channel = None
    
    logger.info("Product Service: Application startup sequence completed.")

@app.on_event("shutdown")
async def shutdown_event():
    """Handles application shutdown, gracefully closing connections."""
    logger.info("Product Service: Application shutdown sequence initiated.")
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        await rabbitmq_connection.close()
        logger.info("Product Service: RabbitMQ connection closed.")

# --- Messaging Helper Functions ---

async def publish_stock_update(product: Product):
    """Publishes a stock update message to RabbitMQ if the connection is available."""
    if not stock_update_channel:
        logger.warning(f"Skipping stock update for Product ID {product.id}: RabbitMQ is not connected.")
        return

    message_body = json.dumps({
        "product_id": product.id,
        "new_stock": product.stock,
        "timestamp": datetime.utcnow().isoformat(),
    })
    message = aio_pika.Message(
        body=message_body.encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        content_type="application/json",
    )
    try:
        exchange = await stock_update_channel.get_exchange("product_events")
        await exchange.publish(message, routing_key="stock.updated")
        logger.info(f"Published stock update for Product ID {product.id}")
    except Exception as e:
        logger.error(f"Failed to publish stock update for Product ID {product.id}. Error: {e}", exc_info=True)

# --- API Endpoints ---

@app.get("/", status_code=status.HTTP_200_OK, summary="Root endpoint")
async def read_root():
    return {"message": "Welcome to the Product Service!"}

@app.get("/health", status_code=status.HTTP_200_OK, summary="Health check endpoint")
async def health_check():
    return {"status": "ok", "service": "product-service"}

@app.post("/products/", response_model=ProductResponse, status_code=status.HTTP_201_CREATED)
async def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    """Creates a new product and publishes a stock update event."""
    try:
        db_product = Product(**product.model_dump())
        db.add(db_product)
        db.commit()
        db.refresh(db_product)
        await publish_stock_update(db_product)
        return db_product
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Product with this name or SKU already exists.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating product: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not create product.")

@app.get("/products/", response_model=List[ProductResponse])
def list_products(db: Session = Depends(get_db), skip: int = 0, limit: int = 100):
    """Retrieves a list of products."""
    return db.query(Product).offset(skip).limit(limit).all()

@app.get("/products/{product_id}", response_model=ProductResponse)
def get_product(product_id: int, db: Session = Depends(get_db)):
    """Retrieves a single product by its ID."""
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    return product

@app.put("/products/{product_id}", response_model=ProductResponse)
async def update_product_details(product_id: int, product_update: ProductUpdate, db: Session = Depends(get_db)):
    """Updates a product's details and publishes a stock update event."""
    db_product = db.query(Product).filter(Product.id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    
    update_data = product_update.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_product, key, value)
    
    db.commit()
    db.refresh(db_product)
    await publish_stock_update(db_product)
    return db_product

@app.delete("/products/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_product(product_id: int, db: Session = Depends(get_db)):
    """Deletes a product."""
    db_product = db.query(Product).filter(Product.id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    
    db.delete(db_product)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/products/deduct-stock", status_code=status.HTTP_200_OK)
async def deduct_stock(request: StockDeductRequest, db: Session = Depends(get_db)):
    """Deducts stock for a given product ID."""
    db_product = db.query(Product).filter(Product.id == request.product_id).first()
    if not db_product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    
    if db_product.stock < request.quantity:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insufficient stock")
        
    db_product.stock -= request.quantity
    db.commit()
    db.refresh(db_product)
    await publish_stock_update(db_product)
    return {"message": f"Stock for product {request.product_id} deducted successfully."}

@app.post("/products/{product_id}/upload-image", response_model=ProductResponse)
async def upload_product_image(product_id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Handles image uploads for a specific product to Azure Blob Storage."""
    if not blob_service_client:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Image upload service is not configured or available.")

    db_product = db.query(Product).filter(Product.id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")

    try:
        # Sanitize filename and create a unique blob name
        file_extension = os.path.splitext(file.filename)[1]
        blob_name = f"product_{product_id}_{int(datetime.utcnow().timestamp())}{file_extension}"
        
        blob_client = blob_service_client.get_blob_client(container=AZURE_STORAGE_CONTAINER_NAME, blob=blob_name)
        
        # Set content type for proper browser rendering
        content_settings = ContentSettings(content_type=file.content_type)
        
        # Upload the file
        contents = await file.read()
        blob_client.upload_blob(contents, content_settings=content_settings)
        
        # Update the product's image URL
        db_product.image_url = blob_client.url
        db.commit()
        db.refresh(db_product)
        
        return db_product
    except AzureError as e:
        logger.error(f"Azure Storage error during image upload for product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to upload image to cloud storage.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during image upload for product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during image upload.")

    # Basic file type validation
    allowed_content_types = ["image/jpeg", "image/png", "image/gif"]
    if file.content_type not in allowed_content_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file type. Only {', '.join(allowed_content_types)} are allowed.",
        )

    try:
        # Create a unique blob name (e.g., product_id/timestamp_originalfilename.ext)
        file_extension = (
            os.path.splitext(file.filename)[1]
            if os.path.splitext(file.filename)[1]
            else ".jpg"
        )  # Ensure extension
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        blob_name = f"{timestamp}{file_extension}"

        blob_client = blob_service_client.get_blob_client(
            container=AZURE_STORAGE_CONTAINER_NAME, blob=blob_name
        )

        logger.info(
            f"Product Service: Uploading image '{file.filename}' for product {product_id} as '{blob_name}' to Azure."
        )

        blob_client.upload_blob(
            file.file,
            overwrite=True,
            content_settings=ContentSettings(content_type=file.content_type),
        )

        sas_token = generate_blob_sas(
            account_name=AZURE_STORAGE_ACCOUNT_NAME,
            account_key=AZURE_STORAGE_ACCOUNT_KEY,
            container_name=AZURE_STORAGE_CONTAINER_NAME,
            blob_name=blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=AZURE_SAS_TOKEN_EXPIRY_HOURS),
        )
        # Construct the full URL with SAS token
        image_url = f"{blob_client.url}?{sas_token}"

        # Update the product in the database with the image URL (including SAS token)
        db_product.image_url = image_url
        db.add(db_product)
        db.commit()
        db.refresh(db_product)

        logger.info(
            f"Product Service: Image uploaded and product {product_id} updated with SAS URL: {image_url}"
        )
        return db_product

    except Exception as e:
        db.rollback()
        logger.error(
            f"Product Service: Error uploading image for product {product_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not upload image or update product: {e}",
        )


# --- Endpoint for Stock Deduction ---
@app.patch(
    "/products/{product_id}/deduct-stock",
    response_model=ProductResponse,
    summary="[DEPRECATED/FALLBACK] Deduct stock quantity for a product (prefer async events)",
)
async def deduct_product_stock_sync(
    product_id: int, request: StockDeductRequest, db: Session = Depends(get_db)
):
    logger.info(
        f"Product Service: Attempting to deduct {request.quantity_to_deduct} from stock for product ID: {product_id}"
    )
    db_product = db.query(Product).filter(Product.product_id == product_id).first()

    if not db_product:
        logger.warning(
            f"Product Service: Stock deduction failed: Product with ID {product_id} not found."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Product not found"
        )

    if db_product.stock_quantity < request.quantity_to_deduct:
        logger.warning(
            f"Product Service: Stock deduction failed for product {product_id}. Insufficient stock: {db_product.stock_quantity} available, {request.quantity_to_deduct} requested."
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Insufficient stock for product '{db_product.name}'. Only {db_product.stock_quantity} available.",
        )

    # Perform deduction
    db_product.stock_quantity -= request.quantity_to_deduct

    try:
        db.add(db_product)
        db.commit()
        db.refresh(db_product)
        logger.info(
            f"Product Service: Stock for product {product_id} updated to {db_product.stock_quantity}. Deducted {request.quantity_to_deduct}."
        )

        # Optional: Log or trigger alert if stock falls below threshold
        if db_product.stock_quantity < RESTOCK_THRESHOLD:
            logger.warning(
                f"Product Service: ALERT! Stock for product '{db_product.name}' (ID: {db_product.product_id}) is low: {db_product.stock_quantity}."
            )

        return db_product
    except Exception as e:
        db.rollback()
        logger.error(
            f"Product Service: Error deducting stock for product {product_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not deduct stock.",
        )
