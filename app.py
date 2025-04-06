from flask import Flask, request, jsonify
import datetime
import pymongo
from bson.binary import Binary
from werkzeug.utils import secure_filename
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("pdf_upload.log"), 
                              logging.StreamHandler()])
logger = logging.getLogger(__name__)

# MongoDB configuration
MONGO_URI = "mongodb://admin:admin@192.168.56.10:27017/?authSource=admin"
DB_NAME = "mydatabase"
COLLECTION_NAME = "power_documents"


# Create a persistent MongoDB client to reuse connections
mongo_client = None

def get_mongo_client():
    global mongo_client
    if mongo_client is None:
        mongo_client = pymongo.MongoClient(MONGO_URI)
    return mongo_client

@app.route('/upload_pdf', methods=['POST'])
def upload_pdf_to_mongodb():
    """
    Flask endpoint to receive a PDF file and store it in MongoDB
    """
    if 'file' not in request.files:
        logger.error("No file part in the request")
        return jsonify({
            "status": "error",
            "message": "No file part in the request"
        }), 400
    
    pdf_file = request.files['file']
    
    if pdf_file.filename == '':
        logger.error("No file selected")
        return jsonify({
            "status": "error",
            "message": "No file selected"
        }), 400
    
    if not pdf_file.filename.lower().endswith('.pdf'):
        logger.error(f"File {pdf_file.filename} is not a PDF")
        return jsonify({
            "status": "error",
            "message": "Only PDF files are allowed"
        }), 400
    
    try:
        # Get MongoDB client
        client = get_mongo_client()
        db = client[DB_NAME]
        
        # Create collection if it doesn't exist
        if COLLECTION_NAME not in db.list_collection_names():
            db.create_collection(COLLECTION_NAME)
        
        # Get secure filename and read file data
        filename = secure_filename(pdf_file.filename)
        pdf_data = pdf_file.read()
        
        # Check if file already exists by filename
        existing = db[COLLECTION_NAME].find_one({"filename": filename})
        
        # Prepare PDF document
        pdf_document = {
            "filename": filename,
            "content_type": "application/pdf",
            "upload_date": datetime.datetime.now(),
            "file_size": len(pdf_data),
            "pdf_data": Binary(pdf_data)  # Store PDF as Binary data
        }
        
        if existing:
            # Update existing document
            db[COLLECTION_NAME].update_one(
                {"filename": filename},
                {"$set": pdf_document}
            )
            logger.info(f"Updated {filename} in MongoDB with new content")
            result = {
                "status": "success",
                "message": f"Updated {filename} in MongoDB with new content",
                "action": "updated"
            }
        else:
            # Insert new document
            db[COLLECTION_NAME].insert_one(pdf_document)
            logger.info(f"Uploaded {filename} to MongoDB")
            result = {
                "status": "success",
                "message": f"Uploaded {filename} to MongoDB",
                "action": "uploaded"
            }
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error processing file {pdf_file.filename}: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error processing file: {str(e)}"
        }), 500

@app.route('/batch_upload', methods=['POST'])
def batch_upload_pdfs():
    """
    Endpoint for future enhancement to handle multiple files in one request
    """
    if 'files' not in request.files:
        return jsonify({
            "status": "error",
            "message": "No files part in the request"
        }), 400
    
    files = request.files.getlist('files')
    
    if len(files) == 0 or files[0].filename == '':
        return jsonify({
            "status": "error",
            "message": "No files selected"
        }), 400
    
    results = []
    
    try:
        client = get_mongo_client()
        db = client[DB_NAME]
        
        if COLLECTION_NAME not in db.list_collection_names():
            db.create_collection(COLLECTION_NAME)
        
        for pdf_file in files:
            if not pdf_file.filename.lower().endswith('.pdf'):
                results.append({
                    "filename": pdf_file.filename,
                    "status": "skipped",
                    "message": "Not a PDF file"
                })
                continue
                
            filename = secure_filename(pdf_file.filename)
            pdf_data = pdf_file.read()
            
            existing = db[COLLECTION_NAME].find_one({"filename": filename})
            
            pdf_document = {
                "filename": filename,
                "content_type": "application/pdf",
                "upload_date": datetime.datetime.now(),
                "file_size": len(pdf_data),
                "pdf_data": Binary(pdf_data)
            }
            
            if existing:
                db[COLLECTION_NAME].update_one(
                    {"filename": filename},
                    {"$set": pdf_document}
                )
                logger.info(f"Updated {filename} in MongoDB with new content")
                results.append({
                    "filename": filename,
                    "status": "success",
                    "action": "updated"
                })
            else:
                db[COLLECTION_NAME].insert_one(pdf_document)
                logger.info(f"Uploaded {filename} to MongoDB")
                results.append({
                    "filename": filename,
                    "status": "success",
                    "action": "uploaded"
                })
                
        return jsonify({
            "status": "success",
            "processed_files": len(results),
            "details": results
        }), 200
            
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error processing files: {str(e)}",
            "partial_results": results
        }),500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5050)
