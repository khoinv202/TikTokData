import abc
import os
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple, Any

# Abstract base classes to follow SOLID principles

class DataSource(abc.ABC):
    """Abstract base class for data sources"""
    @abc.abstractmethod
    def get_links(self) -> List[str]:
        """Retrieves links from a data source"""
        pass

class DataFetcher(abc.ABC):
    """Abstract base class for data fetchers"""
    @abc.abstractmethod
    def fetch_data(self, link: str, output_path: str) -> pd.DataFrame:
        """Fetches data from a link and returns it as a DataFrame"""
        pass

class DataProcessor(abc.ABC):
    """Abstract base class for data processors"""
    @abc.abstractmethod
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processes data and returns a formatted DataFrame"""
        pass

class DataStorage(abc.ABC):
    """Abstract base class for data storage"""
    @abc.abstractmethod
    def save_data(self, df: pd.DataFrame, filename: str) -> str:
        """Saves data to storage and returns the path or id"""
        pass

# Concrete implementations

class GoogleSheetsDataSource(DataSource):
    """Retrieves TikTok links from Google Sheets"""
    def __init__(self):
        self.client = self._connect_to_google_sheets()
        
    def _connect_to_google_sheets(self):
        """Connect to Google Sheets API"""
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        
        scope = ['https://spreadsheets.google.com/feeds', 
                'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            "D:/4handy/Python/n8n-3-452909-dcc8b437ed91.json", scope)
        client = gspread.authorize(credentials)
        return client
        
    def get_links(self) -> List[str]:
        """Get TikTok links from Google Sheet"""
        # Open the spreadsheet by name
        sheet = self.client.open("Ecom - Booking KOC Tiktok - Abby Official Plan")
        
        # Select the specific worksheet
        worksheet = sheet.worksheet("W13 (24/3 - 30/3)")
        
        # Get all values from the specified column
        column_values = worksheet.col_values(11)  # Column K is the 11th column
        
        # Filter out empty values and non-TikTok links
        tiktok_links = [link for link in column_values if 'tiktok.com' in str(link)]
        
        return tiktok_links

class TikTokDataFetcher(DataFetcher):
    """Fetches data from TikTok links"""
    def __init__(self, wait_time: int = 5):
        """Initialize with wait time between requests"""
        self.wait_time = wait_time
        self._setup_browser()
        
    def _setup_browser(self):
        """Set up browser for pyktok"""
        import pyktok as pyk
        pyk.specify_browser('firefox')
        
    def fetch_data(self, link: str, output_path: str) -> pd.DataFrame:
        """Fetch data from a TikTok link"""
        try:
            import pyktok as pyk
            
            # Download TikTok data
            pyk.save_tiktok(link, True, output_path)
            
            # Read the data from the file
            data = pd.read_csv(output_path)
            
            # Add the source link as a column
            data['source_link'] = link
                
            return data
        
        except Exception as e:
            print(f"Error fetching data from {link}: {str(e)}")
            # Return empty DataFrame in case of error
            return pd.DataFrame()

class TikTokDataProcessor(DataProcessor):
    """Processes TikTok data"""
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and format TikTok data"""
        if df.empty:
            return pd.DataFrame()
            
        formatted_df = pd.DataFrame()
        formatted_df['STT'] = range(1, len(df) + 1)
        formatted_df['Link Video'] = df['source_link']
        
        # Handle different possible column names for view count
        if 'video_playcount' in df.columns:
            formatted_df['Lượt xem'] = df['video_playcount']
        elif 'stats_playCount' in df.columns:
            formatted_df['Lượt xem'] = df['stats_playCount']
        elif 'play_count' in df.columns:
            formatted_df['Lượt xem'] = df['play_count']
        else:
            formatted_df['Lượt xem'] = 0
        
        # Handle different possible column names for author
        if 'author_username' in df.columns:
            formatted_df['Người Đăng'] = df['author_username']
        elif 'author_uniqueId' in df.columns:
            formatted_df['Người Đăng'] = df['author_uniqueId']
        elif 'author_nickname' in df.columns:
            formatted_df['Người Đăng'] = df['author_nickname']
        else:
            formatted_df['Người Đăng'] = 'Unknown'
        
        # Handle different possible column names for follower count
        if 'author_followercount' in df.columns:
            formatted_df['Follower Người Đăng'] = df['author_followercount']
        elif 'authorStats_followerCount' in df.columns:
            formatted_df['Follower Người Đăng'] = df['authorStats_followerCount']
        else:
            formatted_df['Follower Người Đăng'] = 0
        
        return formatted_df

class CSVDataStorage(DataStorage):
    """Stores data in CSV files"""
    def __init__(self, output_dir: str = "tiktok_data"):
        """Initialize with output directory"""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def save_data(self, df: pd.DataFrame, filename: str) -> str:
        """Save data to a CSV file"""
        if df.empty:
            return ""
            
        # Ensure file path is correct
        filepath = os.path.join(self.output_dir, filename)
            
        # Save to CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"Data saved to: {filepath}")
        
        return filepath

class GoogleSheetsDataStorage(DataStorage):
    """Stores data in Google Sheets"""
    def __init__(self):
        """Initialize the storage"""
        pass
        
    def _connect_to_google_sheets(self):
        """Connect to Google Sheets API"""
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        
        scope = ['https://spreadsheets.google.com/feeds', 
                'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            "D:/4handy/Python/n8n-3-452909-dcc8b437ed91.json", scope)
        client = gspread.authorize(credentials)
        return client
        
    def save_data(self, df: pd.DataFrame, sheet_name: str) -> str:
        """Save data to a Google Sheet"""
        if df.empty:
            return ""
            
        try:
            client = self._connect_to_google_sheets()
            
            # Create a new spreadsheet
            spreadsheet = client.create(sheet_name)
            spreadsheet_id = spreadsheet.id
            
            # Make it public (anyone with the link can view)
            client.insert_permission(
                spreadsheet_id,
                None,  # No specific user email
                perm_type='anyone',
                role='reader'
            )
            
            # Get the first worksheet
            worksheet = spreadsheet.get_worksheet(0)
            
            # Convert DataFrame to list of lists for uploading
            values = [df.columns.tolist()]  # First row is header
            values.extend(df.values.tolist())  # Add data rows
            
            # Update the worksheet with the values
            worksheet.update('A1', values)
            
            print(f"Data successfully uploaded to Google Sheets")
            sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?usp=sharing"
            print(f"Public link: {sheet_url}")
            
            return spreadsheet_id
            
        except Exception as e:
            print(f"Error uploading to Google Sheets: {str(e)}")
            return ""

class DataCollector:
    """Coordinates the data collection process"""
    def __init__(self, 
                data_source: DataSource, 
                data_fetcher: DataFetcher,
                data_processor: DataProcessor,
                csv_storage: DataStorage,
                sheets_storage: DataStorage):
        """Initialize with components"""
        self.data_source = data_source
        self.data_fetcher = data_fetcher
        self.data_processor = data_processor
        self.csv_storage = csv_storage
        self.sheets_storage = sheets_storage
        
    def collect_data(self) -> Tuple[pd.DataFrame, List[Dict]]:
        """Collect and process data"""
        # Get links from data source
        links = self.data_source.get_links()
        print(f"Found {len(links)} links to process")
        
        # Initialize an empty DataFrame to hold all data
        all_data = pd.DataFrame()
        
        # Create a directory for output if it doesn't exist
        output_dir = "tiktok_data"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create a timestamp for the output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Process each link
        results = []
        
        for i, link in enumerate(links):
            try:
                print(f"Processing link {i+1}/{len(links)}: {link}")
                
                # Create a temporary filename for this link's data
                temp_filename = f"{output_dir}/temp_tiktok_data_{i+1}.csv"
                
                # Fetch data from the link
                temp_data = self.data_fetcher.fetch_data(link, temp_filename)
                
                if not temp_data.empty:
                    # Add link index
                    temp_data['link_index'] = i+1
                    
                    # Append to the combined DataFrame
                    all_data = pd.concat([all_data, temp_data], ignore_index=True)
                    
                    # Add to results
                    results.append({
                        "index": i+1,
                        "link": link,
                        "status": "Success"
                    })
                else:
                    results.append({
                        "index": i+1,
                        "link": link,
                        "status": "Failed",
                        "error": "Empty data returned"
                    })
                
                # Clean up temporary file
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
                    
                # Add 5 second sleep between requests to avoid being rate limited
                if i < len(links) - 1:  # Don't sleep after the last item
                    print(f"Waiting 5 seconds before processing next link...")
                    time.sleep(5)
                    
            except Exception as e:
                print(f"Error processing link {link}: {str(e)}")
                results.append({
                    "index": i+1,
                    "link": link,
                    "status": "Failed",
                    "error": str(e)
                })
                
                # Still sleep after errors
                if i < len(links) - 1:
                    print(f"Waiting 5 seconds before processing next link...")
                    time.sleep(5)
        
        # Save all raw data to a single file
        if not all_data.empty:
            combined_filename = f"all_tiktok_data_{timestamp}.csv"
            self.csv_storage.save_data(all_data, combined_filename)
        
        # Create a summary report
        summary_df = pd.DataFrame(results)
        self.csv_storage.save_data(summary_df, f"processing_summary_{timestamp}.csv")
        
        # Process all data
        if not all_data.empty:
            processed_data = self.data_processor.process_data(all_data)
            
            # Save processed data to CSV
            formatted_csv = f"formatted_tiktok_data_{timestamp}.csv"
            self.csv_storage.save_data(processed_data, formatted_csv)
            
            # Save to Google Sheets
            sheet_name = f"TikTok Data Analysis {timestamp}"
            self.sheets_storage.save_data(processed_data, sheet_name)
        
        return all_data, results

# Main function
def main():
    print("Starting TikTok data extraction...")
    
    # Initialize components
    data_source = GoogleSheetsDataSource()
    data_fetcher = TikTokDataFetcher(wait_time=5)
    data_processor = TikTokDataProcessor()
    csv_storage = CSVDataStorage(output_dir="tiktok_data")
    sheets_storage = GoogleSheetsDataStorage()
    
    # Create data collector
    collector = DataCollector(
        data_source=data_source,
        data_fetcher=data_fetcher,
        data_processor=data_processor,
        csv_storage=csv_storage,
        sheets_storage=sheets_storage
    )
    
    # Collect data
    raw_data, results = collector.collect_data()
    
    # Print summary
    success_count = sum(1 for r in results if r["status"] == "Success")
    print(f"Processing complete. Successfully processed {success_count} out of {len(results)} links.")

if __name__ == "__main__":
    main()