import pymongo
from transformers import AutoTokenizer, pipeline
import logging
import time
import traceback

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('news_summary.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

# Konfigurasi MongoDB
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "BeritaPasar"
STOCK_NEWS_COLLECTION = "StockNews"
SUMMARY_COLLECTION = "RingkasanStockNews"

# Inisialisasi model dan tokenizer
tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large-cnn")
summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=-1)

def split_text(text, max_tokens=1024):
    """Split teks berdasarkan token dengan mempertahankan batas kalimat"""
    sentences = [s.strip() + '.' for s in text.split('.') if s.strip()]
    chunks = []
    current_chunk = []
    current_token_count = 0

    for sentence in sentences:
        token_count = len(tokenizer.tokenize(sentence))
        if current_token_count + token_count > max_tokens:
            chunks.append(' '.join(current_chunk))
            current_chunk = [sentence]
            current_token_count = token_count
        else:
            current_chunk.append(sentence)
            current_token_count += token_count

    if current_chunk:
        chunks.append(' '.join(current_chunk))
    return chunks

def summarize_long_text(text, max_length=250, min_length=50):
    """Meringkas teks panjang dengan algoritma split-merge"""
    try:
        token_count = len(tokenizer.tokenize(text))
        
        # Jika teks pendek, langsung ringkas
        if token_count <= 1024:
            return summarizer(
                text,
                max_length=max_length,
                min_length=min_length,
                truncation=True
            )[0]['summary_text']
        
        # Split teks panjang
        chunks = split_text(text)
        chunk_summaries = []
        
        # Ringkas setiap chunk
        for chunk in chunks:
            chunk_summary = summarizer(
                chunk,
                max_length=512,
                min_length=30,
                truncation=True
            )[0]['summary_text']
            chunk_summaries.append(chunk_summary)
        
        # Merge dan ringkas final
        merged_summary = ' '.join(chunk_summaries)
        final_summary = summarizer(
            merged_summary,
            max_length=max_length,
            min_length=min_length,
            truncation=True
        )[0]['summary_text']
        
        return final_summary
    except Exception as e:
        logger.error(f"Error summarization: {str(e)}")
        return ""

def process_news_direct_approach():
    start_time = time.time()
    successful_docs = 0
    total_docs = 0
    index_counter = 1  # Inisialisasi index
    
    try:
        # Koneksi MongoDB
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        source_col = db[STOCK_NEWS_COLLECTION]
        target_col = db[SUMMARY_COLLECTION]
        
        # Reset koleksi target
        target_col.drop()
        
        # Proses semua dokumen
        for doc in source_col.find():
            total_docs += 1
            doc_start = time.time()
            
            try:
                # Proses ringkasan dengan split-merge
                text = doc.get('konten', '').strip()
                summary = summarize_long_text(text) if text else ""
                
                # Simpan ke MongoDB dengan atribut baru
                new_doc = {
                    'index': index_counter,  # Tambahkan index
                    'judul': doc.get('judul', ''),
                    'konten': doc.get('konten', ''),
                    'rangkuman': summary,
                    'waktu': doc.get('waktu', ''),  # Pertahankan atribut waktu
                    'tanggal_artikel': doc.get('tanggal_artikel', '')
                }
                
                target_col.insert_one(new_doc)
                process_time = time.time() - doc_start
                logger.info(f"BERHASIL: Dokumen index {index_counter} diproses dalam {process_time:.2f} detik")
                successful_docs += 1
                index_counter += 1  # Increment index
                
            except Exception as e:
                logger.error(f"GAGAL: Dokumen {doc.get('_id', '')} - {str(e)}")
        
        # Laporan akhir
        total_time = time.time() - start_time
        avg_time = total_time / successful_docs if successful_docs > 0 else 0
        logger.info("\n=== LAPORAN AKHIR ===")
        logger.info(f"Total dokumen diproses: {total_docs}")
        logger.info(f"Berhasil diproses: {successful_docs}")
        logger.info(f"Gagal diproses: {total_docs - successful_docs}")
        logger.info(f"Total waktu: {total_time:.2f} detik")
        logger.info(f"Rata-rata waktu per dokumen: {avg_time:.2f} detik")
        
    except Exception as e:
        logger.error(f"Error fatal: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        client.close()

if __name__ == "__main__":
    process_news_direct_approach()