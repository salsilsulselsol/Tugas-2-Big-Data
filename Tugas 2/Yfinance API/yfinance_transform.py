from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, date_format, row_number, lit
from pyspark.sql.functions import sum, max, min, stddev, count
from pyspark.sql.functions import to_timestamp
from pyspark.sql.window import Window
from pymongo import MongoClient
import csv
import logging
from datetime import datetime
import time

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Konfigurasi MongoDB
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "Yfinance"
DB_FINAL = "Yfinance_Final"
DF_Saham = "Daftar_Saham.csv"

# Baca nama perusahaan (koleksi) dari CSV
def read_collection_names(csv_file):
    try:
        with open(csv_file, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return [row["Nama Perusahaan"] for i, row in enumerate(reader)]
    except Exception as e:
        logger.error(f"Gagal membaca CSV: {e}")
        return []

# Fungsi untuk inisialisasi Spark Session
def create_spark_session(stock_collection_name=None):
    if not stock_collection_name:
        raise ValueError("Nama koleksi tidak boleh kosong")
    
    return (SparkSession.builder
            .appName("Read Stock Data from MongoDB")
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
            .config("spark.mongodb.input.uri", f"{MONGO_URI}/{DB_NAME}.{stock_collection_name}")
            .config("spark.mongodb.read.connection.uri", f"{MONGO_URI}/{DB_NAME}.{stock_collection_name}") 
            .config("spark.mongodb.write.connection.uri", f"{MONGO_URI}/{DB_FINAL}.{stock_collection_name}")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .getOrCreate())

# Fungsi agregasi berdasarkan Spark
def aggregate_period(df, period, label):
    # Format tanggal
    df = df.withColumn("period_key", date_format("Date", period))

    # Lakukan agregasi lengkap
    agg_df = df.groupBy("period_key").agg(
        mean("Open").alias("avg_open"),
        mean("High").alias("avg_high"),
        mean("Low").alias("avg_low"),
        mean("Close").alias("avg_close"),
        mean("Volume").alias("avg_volume"),
        mean("Dividends").alias("avg_dividends"),
        mean("Stock Splits").alias("avg_stock_splits"),
        sum("Open").alias("sum_open"),
        sum("High").alias("sum_high"),
        sum("Low").alias("sum_low"),
        sum("Close").alias("sum_close"),
        sum("Volume").alias("sum_volume"),
        sum("Dividends").alias("sum_dividends"),
        sum("Stock Splits").alias("sum_stock_splits"),
        max("Open").alias("max_open"),
        max("High").alias("max_high"),
        max("Low").alias("max_low"),
        max("Close").alias("max_close"),
        max("Volume").alias("max_volume"),
        max("Dividends").alias("max_dividends"),
        max("Stock Splits").alias("max_stock_splits"),
        min("Open").alias("min_open"),
        min("High").alias("min_high"),
        min("Low").alias("min_low"),
        min("Close").alias("min_close"),
        min("Volume").alias("min_volume"),
        min("Dividends").alias("min_dividends"),
        min("Stock Splits").alias("min_stock_splits"),
        stddev("Open").alias("std_open"),
        stddev("High").alias("std_high"),
        stddev("Low").alias("std_low"),
        stddev("Close").alias("std_close"),
        stddev("Volume").alias("std_volume"),
        stddev("Dividends").alias("std_dividends"),
        stddev("Stock Splits").alias("std_stock_splits"),
        count("*").alias("row_count")
    )

    windowSpec = Window.orderBy("period_key")
    agg_df = agg_df.withColumn(f"{label}_number", row_number().over(windowSpec))
    agg_df = agg_df.withColumn("agg_type", lit(label))
    return agg_df

if __name__ == "__main__":
    # Mulai hitung waktu
    start_time = time.time()

    total_saham_diambil = 0
    total_data_dalam_db = 0

    koleksi_list = read_collection_names(DF_Saham)

    for nama_koleksi in koleksi_list:
        spark = create_spark_session(nama_koleksi)
        spark.sparkContext.setLogLevel("ERROR")

        try:
            logger.info(f"Membaca data dari MongoDB koleksi: {nama_koleksi}")
            
            df = spark.read.format("mongo") \
                .option("uri", f"{MONGO_URI}/{DB_NAME}.{nama_koleksi}") \
                .load()
            
            if "Date" in df.columns:
                df = df.withColumn("Date", to_timestamp("Date"))
                total_saham_diambil += 1

                for period, label in [("yyyy-MM-dd", "day"), ("yyyy-MM", "month"), ("yyyy", "year")]:
                    if label == "day":
                        df_day = df.withColumn("period_key", date_format("Date", period))
                        
                        df_day.write.format("mongo") \
                            .option("uri", f"{MONGO_URI}/{DB_FINAL}.{nama_koleksi}") \
                            .mode("append") \
                            .save()
                        
                        logger.info(f"✅ Data harian disimpan untuk {nama_koleksi}")
                    else:
                        agg_df = aggregate_period(df, period, label)
                        
                        agg_df.write.format("mongo") \
                            .option("uri", f"{MONGO_URI}/{DB_FINAL}.{nama_koleksi}") \
                            .mode("append") \
                            .save()
                        
                        logger.info(f"✅ Agregasi {label} selesai untuk {nama_koleksi}")
                
                total_data_dalam_db += df.count()

            else:
                logger.warning(f"❌ Kolom 'Date' tidak ditemukan pada koleksi {nama_koleksi}")

        except Exception as e:
            logger.error(f"Terjadi kesalahan: {e}")

        
    
    spark.stop()
    logger.info("Spark session dihentikan")

    # Hitung waktu eksekusi
    elapsed_time = time.time() - start_time

    # Ringkasan
    print("\n📊 Ringkasan Pengambilan Data:")
    print(f"📌 Total saham yang berhasil ditransformasi: {total_saham_diambil} dari {len(koleksi_list)} saham")
    print(f"📦 Total data yang berhasil disimpan di MongoDB: {total_data_dalam_db} dokumen")
    print(f"⏳ Waktu eksekusi: {elapsed_time:.2f} detik")
    print("✅ Proses selesai!")
