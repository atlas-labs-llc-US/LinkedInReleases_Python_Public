import  glob, shutil, time, subprocess, sys, os, os.path, re, csv, json, datetime
import pandas as pd
import polars as pl
from art import *

# Set styling                      
Art=text2art("TER",font='block',chr_ignore=True)
Art2=text2art("The Estate Registry",font='cybermedum',chr_ignore=True)
ArtLine=text2art("-"*25,font="cybermedum",chr_ignore=True)
ArtLine=text2art("-"*25,font="cybermedum",chr_ignore=True)
print(Art)
print(Art2)
print(ArtLine)

# Set results env
results_file = "search_results.txt"
results_file_csv = "search_results.csv"
if os.name == "nt":
    os_type = "Windows"
    sourceDir = os.getcwd() + "\\NN\\Archive\\_"
    destinationDir = os.getcwd() + "\\results\\" 
else:
    os_type = "Unix"
    sourceDir = os.getcwd() + "/data"
    destinationDir = os.getcwd() + "/results/" 
print(f"Current OS type: {os_type}")
print(f"Source dir: {sourceDir}")
print(f"Destination dir: {destinationDir}")

totalFileCount: int = 0
csvFileNamesReviewed: [str] = []
incFound: [str] = []
searchTerms: [str] = ["estate-registry", "jiosi"]
lnames: [str] = ["jiosi"]

def save_results_csv(filename, df, directory=destinationDir):
    """
    Save the provided polars DataFrame to a CSV file.
    """
    filepath = os.path.join(directory, filename)
    
    # Use Polars' to_csv function to save DataFrame to CSV
    df.write_csv(filepath)
    
    print(f"Results saved to CSV: {filepath}")       

def search_csv_files_for_term(sourceDir=sourceDir, search_terms=searchTerms, save_file=results_file):
    """
    Search CSV files for specified terms and save the results, displaying the data using pandas for easy viewing.
    """
    totalFileCount = 0
    csvFileNamesReviewed = []
    df_c: pl.DataFrame = None
    error_message = None  # capture any error messages

    print(f'*'*25 + " Search Parameters " + '*'*25)
    print(f'Searching the following directory: {sourceDir}')
    print(f'Searching for the following terms: {search_terms}')
    print(f'*'*25 + "="*15 + '*'*25)

    try:
        for file in os.listdir(sourceDir):
            full_path = os.path.join(sourceDir, file)  # Using full path
            print(f"Searching file: {full_path}")
            if file.endswith(".csv"):
                totalFileCount += 1
                csvFileNamesReviewed.append(full_path)
                
                # Load CSV data into a pandas DataFrame -- Let's try Polars
                # df_pan = pd.read_csv(full_path, header=1)
                # df_pls = pl.read_csv(full_path, has_header=True, truncate_ragged_lines=True, skip_rows=1) # In-memory
                
                df = pl.scan_csv(full_path, skip_rows=1, has_header=True).collect() # Lazily loaded
                cols = df.columns
                print('-'*10)
                print("All emails in file:")
                print(df.select(["Email Address"]).filter(pl.col("Email Address").is_not_null())
                        .group_by('Email Address').count())
                print("All last names in file:")
                print(df.select(["Last Name"]).filter(pl.col("Last Name").is_not_null())
                        .group_by('Last Name').count())
                print("Matches and date found:")
                print(df.select(['Created Date', 'Email Address', 'Last Name']).filter(pl.col("Email Address").str.contains(search_terms[0])))
                
                # Aggregate by "Last Name" and cast to i64
                res = df.select(["Email Address", "Last Name"]).filter(
                    (pl.col("Email Address").str.contains(search_terms[0])) &
                    (pl.col("Email Address").is_not_null())
                ).group_by(["Email Address"]).agg(
                    count_email=pl.col("Email Address").count().cast(pl.Int64)
                )

                # select, filter total count matching search terms && cast to i64
                filtered_df = df.select(["Email Address", "Last Name"]).filter(
                    (pl.col("Email Address").str.contains(search_terms[0])) &
                    (pl.col("Email Address").is_not_null())
                )
                total_count = filtered_df.shape[0]
                print(f"Matches found: {total_count}")
                print('-'*10)
                
                filename_column = pl.DataFrame({
                    "Filename": [file for _ in range(filtered_df.shape[0])]  # Repeats the filename for each row in filtered_df
                })
                filtered_df_with_filename = filtered_df.hstack(filename_column)
                # This is the same DataFrame--CSV save we want like last time with expected quick-peek BAU expected output
                columns_order = ["Filename", "Email Address", "Last Name"]
                filtered_df_ordered = filtered_df_with_filename.select(columns_order)
                df_c = filtered_df_ordered
                                
                # DF with int type for "count_email" for simplicity
                agg_total = pl.DataFrame({
                    "Email Address": ["Total"],
                    "count_email": [int(total_count)]  # cast to int inferred as i64 by polars
                })

                # Concatenated results for roll-up
                rollup_result = pl.concat([res, agg_total])

                print(f'Roll-up results: {rollup_result}')
                print('-'*25)
                print(f'Final results: {df_c}')
                print('-'*10)
                print('-'*40)
                
    except Exception as e:
        # Capture the error message
        error_message = f"An error occurred during the file search: {str(e)}"

    print(f'*'*25 + " Search Results " + '*'*25)
    print(f"Total files reviewed: {totalFileCount}")
    print(f"Files reviewed: {csvFileNamesReviewed}")
    print(f'Match criteria: {search_terms}')
    print(f"Matches found: {df_c.shape[0]}")
    print(f'*'*25 + "="*15 + '*'*25)
    print(f"Saving results to {save_file}")
    print(f'*'*25 + "="*15 + '*'*25)
    print(f'*'*25 + "="*15 + '*'*25)

    save_results_csv(results_file_csv, df_c)

    # If there was an error, print the error message
    if error_message:
        print(error_message)

    return totalFileCount, csvFileNamesReviewed, df_c.shape[0]

total_files, files_reviewed, inc_found = search_csv_files_for_term()

print(f"Total files reviewed: {total_files}")
print(f"Files reviewed: {files_reviewed}")
df_res = pd.read_csv(os.path.join(destinationDir, results_file_csv))
print("\n")
print(f"Matches found:\n {df_res.head()}\n")
print(f"Prelims: {df_res.shape[0]} matches found in {len(files_reviewed)} file[s] reviewed")