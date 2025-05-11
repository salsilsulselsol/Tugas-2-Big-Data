import os
import sys
import logging
import json
import time
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, expr, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('idx_data_processing.log', mode='w')
    ]
)
logger = logging.getLogger(_name_)

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
SOURCE_DATABASE_NAME = "IDXData"
OUTPUT_DATABASE_NAME = "idx_final"

# Performance tracking
class PerformanceTracker:
    def _init_(self):
        self.collection_stats = {}
        self.total_documents = 0
        self.total_time = 0
        
    def add_collection_stat(self, collection_name, document_count, execution_time):
        velocity = document_count / execution_time if execution_time > 0 else 0
        self.collection_stats[collection_name] = {
            'documents': document_count,
            'time_seconds': execution_time,
            'velocity_docs_per_second': velocity
        }
        self.total_documents += document_count
        self.total_time += execution_time
    
    def get_overall_stats(self):
        overall_velocity = self.total_documents / self.total_time if self.total_time > 0 else 0
        return {
            'total_documents': self.total_documents,
            'total_time_seconds': self.total_time,
            'overall_velocity_docs_per_second': overall_velocity
        }
    
    def generate_report(self):
        report = "===== PERFORMANCE REPORT =====\n\n"
        
        # Per collection stats
        report += "COLLECTION STATISTICS:\n"
        report += "-" * 70 + "\n"
        report += f"{'Collection':<20} {'Documents':<12} {'Time (s)':<12} {'Velocity (docs/s)':<20}\n"
        report += "-" * 70 + "\n"
        
        for collection, stats in self.collection_stats.items():
            report += f"{collection:<20} {stats['documents']:<12} {stats['time_seconds']:.2f}s{' ':<8} {stats['velocity_docs_per_second']:.2f}\n"
        
        # Overall stats
        overall = self.get_overall_stats()
        report += "\nOVERALL STATISTICS:\n"
        report += "-" * 70 + "\n"
        report += f"Total documents processed: {overall['total_documents']}\n"
        report += f"Total execution time: {overall['total_time_seconds']:.2f} seconds\n"
        report += f"Overall velocity: {overall['overall_velocity_docs_per_second']:.2f} documents/second\n"
        report += "-" * 70 + "\n"
        
        return report

def create_spark_session():
    """
    Create a simple SparkSession for data transformation
    """
    try:
        spark = SparkSession.builder \
            .appName("IDX Financial Data Processing") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .master("local[*]") \
            .getOrCreate()
        
        logger.info(f"Created Spark session with version: {spark.version}")
        return spark
    
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def get_mongodb_connection():
    """
    Create a MongoDB connection using PyMongo
    """
    try:
        client = MongoClient(MONGO_URI)
        logger.info("Successfully connected to MongoDB")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

def process_financial_data(spark_df):
    """
    Process and transform financial data using Spark to maintain 32 attributes
    """
    try:
        # Step 1: Select base columns and transform
        df_processed = spark_df.select(
            # 1-3: Basic identifiers
            "company_code",
            "year",
            "period",
            
            # 4-6: Company information
            col("data.EntityName").alias("company_name"),
            col("data.Sector").alias("sector"),
            col("data.Subsector").alias("subsector"),
            
            # 7-10: Main financial metrics
            col("data.SalesAndRevenue").cast("double").alias("revenue"),
            col("data.GrossProfit").cast("double").alias("gross_profit"),
            col("data.ProfitLossBeforeIncomeTax").cast("double").alias("profit_before_tax"),
            col("data.FinanceCosts").cast("double").alias("finance_costs"),
            col("data.ProfitLoss").cast("double").alias("net_profit"),
            
            # 11-15: Balance sheet items
            col("data.CashAndCashEquivalents").cast("double").alias("cash"),
            col("data.Assets").cast("double").alias("total_assets"),
            col("data.ShortTermLoans").cast("double").alias("short_term_loans"),
            col("data.CurrentMaturitiesOfBankLoans").cast("double").alias("current_maturities"),
            col("data.LongTermBankLoans").cast("double").alias("long_term_borrowing"),
            col("data.Equity").cast("double").alias("total_equity"),
            
            # 16-18: Cash flow items
            col("data.NetCashFlowsReceivedFromUsedInOperatingActivities").cast("double").alias("cash_from_operations"),
            col("data.NetCashFlowsReceivedFromUsedInInvestingActivities").cast("double").alias("cash_from_investing"),
            col("data.NetCashFlowsReceivedFromUsedInFinancingActivities").cast("double").alias("cash_from_financing"),
            
            # 19: Additional balance sheet
            col("data.Liabilities").cast("double").alias("total_liabilities"),
            
            # 20-21: Earnings metrics
            col("data.BasicEarningsLossPerShareFromContinuingOperations").cast("double").alias("basic_eps"),
            
            # 22-24: Expense items
            col("data.SellingExpenses").cast("double").alias("selling_expenses"),
            col("data.GeneralAndAdministrativeExpenses").cast("double").alias("g_and_a_expenses"),
            
            # 25-26: Current assets and liabilities
            col("data.CurrentAssets").cast("double").alias("current_assets"),
            col("data.CurrentLiabilities").cast("double").alias("current_liabilities"),
        )
        
        # Replace null values with 0 for numeric columns
        all_numeric_cols = [
            "revenue", "gross_profit", "profit_before_tax", "finance_costs", "net_profit",
            "cash", "total_assets", "short_term_loans", "current_maturities", "long_term_borrowing",
            "total_equity", "cash_from_operations", "cash_from_investing", "cash_from_financing",
            "total_liabilities", "basic_eps", "selling_expenses", "g_and_a_expenses",
            "current_assets", "current_liabilities"
        ]
        
        for col_name in all_numeric_cols:
            df_processed = df_processed.fillna(0, subset=[col_name])
        
        # Step 2: Add calculated fields
        
        # 9: Calculate operating_profit from profit_before_tax and finance_costs
        df_processed = df_processed.withColumn(
            "operating_profit",
            expr("profit_before_tax - finance_costs")
        )
        
        # 13: Calculate short_term_borrowing (use ShortTermLoans or CurrentMaturitiesOfBankLoans if null)
        df_processed = df_processed.withColumn(
            "short_term_borrowing",
            coalesce(col("short_term_loans"), col("current_maturities"), lit(0))
        )
        
        # 20: Calculate EBITDA
        df_processed = df_processed.withColumn(
            "ebitda",
            expr("profit_before_tax + finance_costs")
        )
        
        # 24: Calculate operating_expenses
        df_processed = df_processed.withColumn(
            "operating_expenses",
            expr("selling_expenses + g_and_a_expenses")
        )
        
        # 27: Calculate current_ratio
        df_processed = df_processed.withColumn(
            "current_ratio",
            expr("current_assets / nullif(current_liabilities, 0)")
        )
        
        # 28: Calculate asset_to_equity_ratio
        df_processed = df_processed.withColumn(
            "asset_to_equity_ratio",
            expr("total_assets / nullif(total_equity, 0)")
        )
        
        # 29: Calculate debt_to_equity_ratio
        df_processed = df_processed.withColumn(
            "debt_to_equity_ratio",
            expr("total_liabilities / nullif(total_equity, 0)")
        )
        
        # 30: Calculate gross_margin_pct
        df_processed = df_processed.withColumn(
            "gross_margin_pct",
            expr("gross_profit / nullif(revenue, 0) * 100")
        )
        
        # 31: Calculate operating_margin_pct
        df_processed = df_processed.withColumn(
            "operating_margin_pct",
            expr("operating_profit / nullif(revenue, 0) * 100")
        )
        
        # 32: Calculate net_margin_pct
        df_processed = df_processed.withColumn(
            "net_margin_pct",
            expr("net_profit / nullif(revenue, 0) * 100")
        )
        
        # Step 3: Select final 32 columns in the specified order
        final_df = df_processed.select(
            "company_code",
            "year",
            "period",
            "company_name",
            "sector",
            "subsector",
            "revenue",
            "gross_profit",
            "operating_profit",
            "net_profit",
            "cash",
            "total_assets",
            "short_term_borrowing",
            "long_term_borrowing",
            "total_equity",
            "cash_from_operations",
            "cash_from_investing",
            "cash_from_financing",
            "total_liabilities",
            "ebitda",
            "basic_eps",
            "selling_expenses",
            "g_and_a_expenses",
            "operating_expenses",
            "current_assets",
            "current_liabilities",
            "current_ratio",
            "asset_to_equity_ratio",
            "debt_to_equity_ratio",
            "gross_margin_pct",
            "operating_margin_pct",
            "net_margin_pct"
        )
        
        return final_df
    
    except Exception as e:
        logger.error(f"Error in data processing: {str(e)}")
        raise

def process_collection(spark, mongo_client, collection_name, performance_tracker):
    """
    Process a specific MongoDB collection using PyMongo and Spark
    """
    start_time = time.time()
    document_count = 0
    
    try:
        logger.info(f"Processing collection: {collection_name}")
        
        # Step 1: Read data from MongoDB using PyMongo
        source_db = mongo_client[SOURCE_DATABASE_NAME]
        output_db = mongo_client[OUTPUT_DATABASE_NAME]
        
        # Check if collection exists
        if collection_name not in source_db.list_collection_names():
            logger.warning(f"Collection {collection_name} not found in source database")
            return 0
        
        source_collection = source_db[collection_name]
        
        # Fetch data from MongoDB
        mongo_data = list(source_collection.find())
        document_count = len(mongo_data)
        logger.info(f"Total records read: {document_count}")
        
        # Handle MongoDB ObjectId for JSON serialization
        for doc in mongo_data:
            doc['_id'] = str(doc['_id'])
        
        # Convert to pandas
        df_pandas = pd.DataFrame(mongo_data)
        
        # Step 2: Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df_pandas)
        logger.info("Successfully converted MongoDB data to Spark DataFrame")
        
        # Step 3: Process data using Spark
        processed_df = process_financial_data(spark_df)
        processed_count = processed_df.count()
        logger.info(f"Processed records: {processed_count}")
        
        # Step 4: Convert back to Pandas for MongoDB storage
        pandas_processed = processed_df.toPandas()
        
        # Step 5: Write processed data back to MongoDB using PyMongo
        output_collection = output_db[f"{collection_name}_final"]
        
        # Clear existing data
        output_collection.drop()
        
        # Convert pandas DataFrame to list of dictionaries
        records = pandas_processed.to_dict('records')
        
        # Insert data in batch
        if records:
            output_collection.insert_many(records)
            logger.info(f"Saved {len(records)} records to {collection_name}_final collection")
        else:
            logger.warning("No records to insert after processing")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Processing time for {collection_name}: {elapsed_time:.2f} seconds")
        
        # Calculate documents per second
        docs_per_second = document_count / elapsed_time if elapsed_time > 0 else 0
        logger.info(f"Processing velocity for {collection_name}: {docs_per_second:.2f} documents/second")
        
        # Record performance statistics
        performance_tracker.add_collection_stat(collection_name, document_count, elapsed_time)
        
        logger.info(f"Successfully processed {collection_name}")
        return document_count
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Error processing collection {collection_name}: {str(e)}")
        
        # Record failed performance statistics
        performance_tracker.add_collection_stat(collection_name, document_count, elapsed_time)
        raise

def main():
    """
    Main function to process multiple financial data collections
    """
    spark = None
    mongo_client = None
    performance_tracker = PerformanceTracker()
    total_start_time = time.time()
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Connect to MongoDB
        mongo_client = get_mongodb_connection()
        
        # Create output database if it doesn't exist
        if OUTPUT_DATABASE_NAME not in mongo_client.list_database_names():
            logger.info(f"Creating output database: {OUTPUT_DATABASE_NAME}")
        
        # List of collections to process
        collections = ["Financial_2021", "Financial_2022", "Financial_2023", "Financial_2024"]
        
        total_docs = 0
        
        # Process each collection
        for collection in collections:
            docs = process_collection(spark, mongo_client, collection, performance_tracker)
            total_docs += docs
        
        total_elapsed_time = time.time() - total_start_time
        
        # Generate performance report
        report = performance_tracker.generate_report()
        
        # Write report to file
        with open('performance_report.txt', 'w') as f:
            f.write(report)
        
        # Also print to console
        print("\n" + report)
        
        logger.info(f"Data processing completed successfully!")
        logger.info(f"Total documents processed: {total_docs}")
        logger.info(f"Total execution time: {total_elapsed_time:.2f} seconds")
        logger.info(f"Overall processing velocity: {total_docs/total_elapsed_time:.2f} documents/second")
    
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}")
        raise
    
    finally:
        # Close connections
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed")
        
        if spark:
            spark.stop()
            logger.info("Spark session closed")

if _name_ == "_main_":
    main()
