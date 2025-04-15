import pyktok as pyk
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime

# Set up browser for pyktok
pyk.specify_browser('firefox')  # Adjust as needed for your environment

# Set up Google Sheets API
def connect_to_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name("D:/4handy/Python/n8n-3-452909-dcc8b437ed91.json", scope)
    client = gspread.authorize(credentials)
    return client

# Get TikTok links from Google Sheet
def get_tiktok_links():
    client = connect_to_google_sheets()
    
    # Open the spreadsheet by name
    sheet = client.open("Ecom - Booking KOC Tiktok - Abby Official Plan")
    
    # Select the specific worksheet
    worksheet = sheet.worksheet("W13 (24/3 - 30/3)")
    
    # Get all values from column K (11th column, 0-indexed)
    column_k = worksheet.col_values(11)  # Column K is the 11th column
    
    # Filter out empty values and headers
    tiktok_links = [link for link in column_k if 'tiktok.com' in link]
    
    return tiktok_links

# Process TikTok links and save data to a single file
def process_tiktok_links(links):
    # Create a directory for output if it doesn't exist
    output_dir = "tiktok_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a timestamp for the output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_filename = f"{output_dir}/all_tiktok_data_{timestamp}.csv"
    
    # Initialize an empty DataFrame to hold all data
    all_data = pd.DataFrame()
    
    # Process each link
    results = []
    for i, link in enumerate(links):
        try:
            print(f"Processing link {i+1}/{len(links)}: {link}")
            
            # Create a temporary filename for this link's data
            temp_filename = f"{output_dir}/temp_tiktok_data_{i+1}.csv"
            
            # Download TikTok data
            pyk.save_tiktok(link, True, temp_filename)
            
            # Read the data from the temp file
            temp_data = pd.read_csv(temp_filename)
            
            # Add the source link as a column
            temp_data['source_link'] = link
            temp_data['link_index'] = i+1
            
            # Append to the combined DataFrame
            all_data = pd.concat([all_data, temp_data], ignore_index=True)
            
            # Delete the temporary file
            os.remove(temp_filename)
            
            results.append({
                "index": i+1, 
                "link": link, 
                "status": "Success"
            })
            
        except Exception as e:
            print(f"Error processing link {link}: {str(e)}")
            results.append({
                "index": i+1, 
                "link": link, 
                "status": "Failed", 
                "error": str(e)
            })
    
    # Save all data to a single file
    if not all_data.empty:
        all_data.to_csv(combined_filename, index=False)
        print(f"All data saved to {combined_filename}")
    
    # Create a summary report
    summary_df = pd.DataFrame(results)
    summary_df.to_csv(f"{output_dir}/processing_summary_{timestamp}.csv", index=False)
    
    return results, combined_filename

# Main function
def main():
    print("Starting TikTok data extraction...")
    
    # Get TikTok links from Google Sheet
    links = get_tiktok_links()
    print(f"Found {len(links)} TikTok links to process")
    
    # Process each link and save to a single file
    results, output_file = process_tiktok_links(links)
    
    # Print summary
    success_count = sum(1 for r in results if r["status"] == "Success")
    print(f"Processing complete. Successfully processed {success_count} out of {len(links)} links.")
    print(f"All data has been combined into: {output_file}")
    print(f"See processing_summary.csv for detailed results.")

if __name__ == "__main__":
    main()
import pandas as pd

# Đọc file CSV gốc
input_file = "C:/Users/wind4/tiktok_data/all_tiktok_data_20250415_092751.csv"
df = pd.read_csv(input_file)

# Tạo cột STT (số thứ tự)
df['STT'] = range(1, len(df) + 1)

# Chọn và đổi tên các cột theo yêu cầu
output_df = df[['STT', 'source_link', 'video_playcount', 'author_username', 'author_followercount']].copy()
output_df.columns = ['STT', 'Link Video', 'Lượt xem', 'Người Đăng', 'Follower Người Đăng']

# Ghi dữ liệu vào file CSV mới
output_file = "formatted_tiktok_data.csv"
output_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"File CSV mới đã được tạo: {output_file}")