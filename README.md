# Bank Market Capitalization ETL Project

This project implements an **ETL (Extract, Transform, Load) pipeline** to process financial data on the world’s largest banks. The goal is to scrape, clean, enrich, and store market capitalization data for further analysis.

## Features
- **Extract**: Scrapes the list of the largest banks by market capitalization from a Wikipedia snapshot.
- **Transform**:  
  - Cleans and converts market capitalization values into numeric format.  
  - Enriches the dataset by converting values from USD into GBP, EUR, and INR using a CSV of exchange rates.  
- **Load**:  
  - Saves the transformed dataset into a CSV file.  
  - Loads the dataset into an SQLite database for structured queries.  

## Workflow
1. **Extraction** – Data is scraped using `requests` and `BeautifulSoup` to build a clean DataFrame with bank names and market capitalization.  
2. **Transformation** – Exchange rates are applied to create additional columns (`MC_GBP_Billions`, `MC_EUR_Billions`, `MC_INR_Billions`).  
3. **Loading** – Data is exported both as a `.csv` file and into a SQLite database.  
4. **Queries** – Example SQL queries are executed to demonstrate data retrieval, such as:  
   - Listing all banks  
   - Computing average market cap in GBP  
   - Retrieving the first 5 banks  

## Tech Stack
- **Python**: Data pipeline implementation  
- **Libraries**: `pandas`, `numpy`, `requests`, `BeautifulSoup`, `sqlite3`  
- **Database**: SQLite for structured storage and queries  

## Logging
The pipeline includes progress logging into `bank_project.txt`, providing timestamps for each stage of execution.

## Example Use Cases
- Data enrichment and reporting on global financial institutions  
- Practice for ETL pipelines using Python  
- Foundation for larger financial analytics projects  

## How to Run
1. Clone this repository:  
   ```bash
   git clone <your-repo-url>
   cd <your-repo-name>
   ```

2. Install dependencies:  
   ```bash
   pip install -r requirements.txt
   ```

3. Run the script:
   ```bash
   python bank_project.py
   ```

5. Check the outputs:  
   - `Largest_banks_data.csv` → CSV file of processed data  
   - `Banks.db` → SQLite database with the table `Largest_banks`  
   - `bank_project.txt` → Log file tracking pipeline progress  

## License
This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.
