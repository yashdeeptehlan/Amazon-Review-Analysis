Amazon Reviews Analysis - Cross-Selling & Customer Overlap

📌 Business Problem

Focus: Cross-Selling & Customer Overlap

This project aims to analyze customer reviews across multiple product categories (Electronics, Books, and Watches) to identify potential cross-selling opportunities and customer purchasing behaviors.

Primary Question:

Are there customers who purchase across multiple categories (Books, Watches, and Electronics)?

Supporting Questions:

Which purchasing patterns exist across categories?

Do customers who buy certain electronics also tend to buy specific books or watches?

Can we identify bundle opportunities or product recommendations?

Is there a correlation between specific electronics and watch purchases that might indicate a potential bundle?

Are there popular book genres frequently bought alongside certain tech items?

Goal:

Leverage the Amazon Reviews dataset to uncover insights into cross-category behavior, ultimately guiding marketing strategies, personalized recommendations, and inventory decisions.

By pinpointing overlaps, the business can target customers with more relevant promotions and optimize stock for high-demand bundle opportunities.

📊 Project Overview

Data Sources:

Amazon Customer Reviews dataset (Electronics, Books, Watches)

Extracted, cleaned, and transformed for cross-category analysis

ETL Pipeline Steps:

Extraction: Load TSV files into Apache Spark for efficient processing.

Transformation:

Data cleaning: Remove duplicates, filter missing values.

Feature engineering: Sentiment analysis, category pivoting.

Aggregation: Customer-level summary metrics (average ratings, review counts, sentiment scores).

Cross-Sell Score: Calculate customer likelihood of purchasing across categories.

Loading: Store processed data into SQL Server & Snowflake for querying and visualization.

🛠️ Tech Stack

Data Processing: Apache Spark, Pandas

Database: SQL Server, Snowflake

Cloud Storage: Azure Blob Storage

Scripting: Python, SQL

Visualization: Power BI, Matplotlib, Seaborn

📂 Repository Structure

📦 Amazon-Review-Analysis
├── 📂 data/                     # Raw & Processed datasets
├── 📂 etl/                      # ETL scripts for processing data
├── 📂 reports/                   # Deliverables, diagrams, documentation
├── 📂 notebooks/                 # Jupyter Notebooks for analysis & visualization
├── clean_upload.py              # Script for uploading cleaned data
├── etl_crossselling.py          # Core ETL logic for cross-sell analysis
├── merge_tsv.py                 # Merges multiple review datasets
├── Readme.md                    # Project documentation
└── .gitignore                    # Excluded files

📈 Key Insights & Next Steps

Identify customer clusters based on purchasing habits.

Create product recommendation rules based on review patterns.

Develop Power BI dashboards for easy visualization of trends.

📩 Contact & Contributions

Have ideas or improvements? Feel free to open an Issue or submit a Pull Request. 🚀

