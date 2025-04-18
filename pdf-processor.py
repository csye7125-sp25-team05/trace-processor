#!/usr/bin/env python3
# pdf_processor.py - Processes PDFs for RAG system

import os
import json
import time
import tempfile
import logging
from typing import List, Dict, Any, Tuple

# Kafka imports
from kafka import KafkaConsumer, KafkaProducer

# Google Cloud imports
from google.cloud import storage
import google.generativeai as genai

# Pinecone imports
import pinecone
from pinecone import ServerlessSpec

# PDF processing imports
from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("pdf-processor")

class PDFProcessor:
    """PDF Processor for RAG system that uses Kafka, Gemini, and Pinecone."""
    
    def _init_(self):
        """Initialize the PDF processor with all necessary clients and configurations."""
        # Initialize GCS client
        self.storage_client = storage.Client()
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME")
        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable not set")
        self.bucket = self.storage_client.bucket(self.bucket_name)
        
        # Initialize Gemini API for embeddings
        gemini_api_key = os.environ.get("GEMINI_API_KEY")
        if not gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        genai.configure(api_key=gemini_api_key)
        self.embedding_model = "models/embedding-001"  # Latest Gemini embedding model
        
        # Initialize Pinecone
        pinecone_api_key = os.environ.get("PINECONE_API_KEY")
        if not pinecone_api_key:
            raise ValueError("PINECONE_API_KEY environment variable not set")
        pinecone.init(api_key=pinecone_api_key)
        
        # Initialize Kafka
        self.kafka_bootstrap_servers = os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", 
            "kafka-headless.kafka.svc.cluster.local:9092"
        )
        self.consumer = self._initialize_kafka_consumer()
        self.producer = self._initialize_kafka_producer()
        
        # Initialize index
        self.index_name = "pdf-embeddings"
        self.index = self._initialize_pinecone_index()
        
        logger.info("PDF Processor initialized successfully")
    
    def _initialize_pinecone_index(self):
        """Initialize Pinecone index, creating it if it doesn't exist."""
        try:
            if self.index_name not in pinecone.list_indexes():
                logger.info(f"Creating new Pinecone index: {self.index_name}")
                # Create a serverless index with free tier settings
                pinecone.create_index(
                    name=self.index_name,
                    dimension=768,  # Dimension for Gemini embeddings
                    metric="cosine",
                    spec=ServerlessSpec(
                        cloud="aws",
                        region="us-east-1"  # Free tier is in AWS us-east-1
                    )
                )
            
            index = pinecone.Index(self.index_name)
            logger.info(f"Connected to Pinecone index: {self.index_name}")
            return index
        except Exception as e:
            logger.error(f"Failed to initialize Pinecone index: {str(e)}")
            raise
    
    def _initialize_kafka_consumer(self):
        """Initialize and configure the Kafka consumer."""
        try:
            consumer = KafkaConsumer(
                'pdf-uploads',
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                group_id='pdf-processor',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                # Optimized consumer configurations for performance
                fetch_max_wait_ms=500,
                fetch_min_bytes=1,
                fetch_max_bytes=52428800,  # 50MB max fetch
                max_partition_fetch_bytes=10485760  # 10MB per partition
            )
            logger.info(f"Kafka consumer initialized with bootstrap servers: {self.kafka_bootstrap_servers}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {str(e)}")
            raise
    
    def _initialize_kafka_producer(self):
        """Initialize and configure the Kafka producer."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                # Optimized producer settings
                batch_size=16384,
                linger_ms=100,
                buffer_memory=33554432  # 32MB buffer
            )
            logger.info(f"Kafka producer initialized with bootstrap servers: {self.kafka_bootstrap_servers}")
            return producer
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}")
            raise
    
    def generate_embeddings(self, texts: List[str], batch_size: int = 16) -> List[List[float]]:
        """Generate embeddings using Gemini API with batching.
        
        Args:
            texts: List of text chunks to embed
            batch_size: Number of texts to process in each batch
            
        Returns:
            List of embedding vectors
        """
        all_embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            try:
                embedding_results = genai.embed_content(
                    model=self.embedding_model,
                    content=batch,
                    task_type="retrieval_document"
                )
                batch_embeddings = [result.embedding for result in embedding_results.embeddings]
                all_embeddings.extend(batch_embeddings)
                
                # Free tier rate limit protection - add a small delay between batches
                if i + batch_size < len(texts):
                    time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error generating embeddings for batch {i//batch_size}: {str(e)}")
                # If we hit a rate limit, wait longer and retry
                if "rate limit" in str(e).lower():
                    logger.info("Rate limit hit, waiting 5 seconds before retrying")
                    time.sleep(5)
                    # Retry with a smaller batch size
                    smaller_batch = batch[:max(1, len(batch)//2)]
                    try:
                        embedding_results = genai.embed_content(
                            model=self.embedding_model,
                            content=smaller_batch,
                            task_type="retrieval_document"
                        )
                        batch_embeddings = [result.embedding for result in embedding_results.embeddings]
                        all_embeddings.extend(batch_embeddings)
                        
                        # Process the remaining half
                        if len(batch) > len(smaller_batch):
                            time.sleep(1)  # Wait a bit longer
                            remaining_batch = batch[len(smaller_batch):]
                            embedding_results = genai.embed_content(
                                model=self.embedding_model,
                                content=remaining_batch,
                                task_type="retrieval_document"
                            )
                            batch_embeddings = [result.embedding for result in embedding_results.embeddings]
                            all_embeddings.extend(batch_embeddings)
                    except Exception as retry_error:
                        logger.error(f"Failed retry for batch {i//batch_size}: {str(retry_error)}")
                        # Return empty embeddings for this batch
                        all_embeddings.extend([[0] * 768] * len(batch))
                else:
                    # For other errors, return empty embeddings
                    all_embeddings.extend([[0] * 768] * len(batch))
        
        return all_embeddings
    
    def process_pdf(self, pdf_path: str, pdf_id: str) -> int:
        """Process a PDF file, generate embeddings, and store in Pinecone.
        
        Args:
            pdf_path: Path to the downloaded PDF file
            pdf_id: Unique ID for the PDF
            
        Returns:
            Number of chunks processed
        """
        logger.info(f"Processing PDF: {pdf_path} (ID: {pdf_id})")
        
        # Load PDF
        loader = PyPDFLoader(pdf_path)
        documents = loader.load()
        
        # Split into chunks - smaller chunks for better retrieval precision
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=512,
            chunk_overlap=50
        )
        chunks = text_splitter.split_documents(documents)
        
        # Extract text content for batch processing
        texts = [chunk.page_content for chunk in chunks]
        logger.info(f"Split PDF into {len(chunks)} chunks")
        
        if not texts:
            logger.warning(f"No text extracted from PDF: {pdf_id}")
            return 0
        
        # Generate embeddings in batch using Gemini API
        embeddings = self.generate_embeddings(texts)
        
        # Prepare vectors for batch upload
        vectors_to_upsert = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            vector_id = f"{pdf_id}-{i}"
            metadata = {
                "text": chunk.page_content,
                "source": pdf_id,
                "page": chunk.metadata.get("page", 0)
            }
            vectors_to_upsert.append((vector_id, embedding, metadata))
        
        # Batch upsert to Pinecone (smaller batches to avoid rate limits on free tier)
        batch_size = 50  # Reduced batch size for free tier
        for i in range(0, len(vectors_to_upsert), batch_size):
            batch = vectors_to_upsert[i:i + batch_size]
            try:
                self.index.upsert(vectors=batch)
                logger.info(f"Upserted batch {i//batch_size + 1}/{(len(vectors_to_upsert) + batch_size - 1)//batch_size}")
                
                # Add a small delay between batches to avoid rate limiting
                if i + batch_size < len(vectors_to_upsert):
                    time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error upserting batch {i//batch_size + 1}: {str(e)}")
                # If we hit a rate limit, wait longer and retry with smaller batch
                if "limit" in str(e).lower():
                    logger.info("Rate limit hit, waiting 5 seconds before retrying")
                    time.sleep(5)
                    # Try with smaller batches
                    smaller_batch_size = max(10, batch_size // 2)
                    for j in range(i, min(i + batch_size, len(vectors_to_upsert)), smaller_batch_size):
                        smaller_batch = vectors_to_upsert[j:j + smaller_batch_size]
                        try:
                            self.index.upsert(vectors=smaller_batch)
                            logger.info(f"Upserted smaller batch {j//smaller_batch_size + 1}")
                            time.sleep(1)  # Wait longer between smaller batches
                        except Exception as retry_error:
                            logger.error(f"Failed retry for smaller batch: {str(retry_error)}")
        
        logger.info(f"Successfully processed PDF: {pdf_id} with {len(chunks)} chunks")
        return len(chunks)
    
    def run(self):
        """Main processing loop that consumes Kafka messages and processes PDFs."""
        logger.info("PDF processor starting up...")
        logger.info("Waiting for messages...")
        
        while True:
            # Poll for messages with timeout
            message_batch = self.consumer.poll(timeout_ms=1000)
            
            if not message_batch:
                continue
                
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    try:
                        pdf_info = message.value
                        pdf_name = pdf_info.get("filename")
                        pdf_id = pdf_info.get("id")
                        
                        logger.info(f"Received message for PDF: {pdf_name} (ID: {pdf_id})")
                        
                        # Create a temporary file to store the downloaded PDF
                        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                            local_path = temp_file.name
                            
                            # Download PDF from GCS
                            try:
                                blob = self.bucket.blob(pdf_name)
                                blob.download_to_filename(local_path)
                                logger.info(f"Downloaded PDF to: {local_path}")
                            except Exception as e:
                                logger.error(f"Failed to download PDF: {str(e)}")
                                self.producer.send('pdf-processing-status', {
                                    "id": pdf_id,
                                    "status": "failed",
                                    "error": f"Failed to download PDF: {str(e)}"
                                })
                                continue
                        
                        try:
                            # Process PDF
                            chunks_processed = self.process_pdf(local_path, pdf_id)
                            
                            # Send success message
                            self.producer.send('pdf-processing-status', {
                                "id": pdf_id,
                                "status": "completed",
                                "chunks_processed": chunks_processed
                            })
                            logger.info(f"Successfully processed {chunks_processed} chunks from {pdf_name}")
                            
                        except Exception as e:
                            # Send failure message
                            error_msg = f"Failed to process {pdf_name}: {str(e)}"
                            logger.error(error_msg)
                            self.producer.send('pdf-processing-status', {
                                "id": pdf_id,
                                "status": "failed",
                                "error": error_msg
                            })
                            
                        finally:
                            # Clean up
                            if os.path.exists(local_path):
                                os.remove(local_path)
                                logger.debug(f"Removed temporary file: {local_path}")
                                
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")

            # Commit offsets
            self.consumer.commit()


if __name__ == "__main__":
    try:
        processor = PDFProcessor()
        processor.run()
    except Exception as e:
        logger.critical(f"Fatal error in PDF processor: {str(e)}")
        raise
