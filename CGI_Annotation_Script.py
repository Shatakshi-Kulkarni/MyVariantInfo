import pandas as pd
import myvariant
import argparse
import os
import re
import time
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type

# Global variable for MyVariantInfo client, will be initialized in process_file
mv = None

# Retry MyVariant query if it fails
@retry(wait=wait_fixed(1), stop=stop_after_attempt(3), retry=retry_if_exception_type(Exception))
def safe_getvariant(variant):
    if mv is None:
        # This is a fallback initialization. The primary one is in process_file.
        global mv_safe_fallback
        mv_safe_fallback = myvariant.MyVariantInfo()
        print("Warning: MyVariantInfo client was not initialized prior to safe_getvariant. Initializing now.")
        return mv_safe_fallback.getvariant(variant, fields='cgi')
    return mv.getvariant(variant, fields='cgi')

def split_variant(variant):
    #Splits various HGVS-like variant formats into components for clear output columns.
    # Pattern 1: Substitution (SNP) like chr1:g.243777040G>T
    snp_match = re.match(r"^(chr\w+:g\.\d+)([ACGT]+)>([ACGT]+)$", variant)
    if snp_match:
        chrom_pos, ref, alt = snp_match.groups()
        return chrom_pos, ref, alt

    # Pattern 2: Deletion or Duplication (single base or range)
    indel_match = re.match(r"^(chr\w+:g\.\d+(?:_\d+)?)(del|dup)$", variant)
    if indel_match:
        chrom_pos, alt = indel_match.groups()
        return chrom_pos, '', alt

    # Pattern 3: Copy Number Alteration (CNA) / Range
    cna_match = re.match(r"^(chr\w+:g\.\d+_\d+)$", variant)
    if cna_match:
        chrom_pos = cna_match.group(0)
        return chrom_pos, 'CNA', ''

    # Fallback: If no specific pattern matches, return the original variant.
    return variant, '', ''


def process_file(file_path):
    file_start_time = time.time() # Start timing for this specific file

    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return False # Indicate failure
    if not file_path.endswith(".xlsx"):
        print(f"Error: File '{file_path}' is not an .xlsx file. Skipping.")
        return False # Indicate failure

    print(f"\nProcessing file: {os.path.basename(file_path)} ...")
    
    global mv 
    mv = myvariant.MyVariantInfo() 
    
    try:
        data = pd.read_excel(file_path)
    except Exception as e:
        print(f"Error reading Excel file {os.path.basename(file_path)}: {e}")
        return False # Indicate failure

    if 'Genomic Alteration' not in data.columns:
        print(f"Error: Column 'Genomic Alteration' not found in {os.path.basename(file_path)}.")
        return False # Indicate failure

    results = []
    variants_processed_in_file = 0
    for idx, row in data.iterrows():
        variant = row['Genomic Alteration']
        if pd.isna(variant) or not isinstance(variant, str):
            continue
            
        chrom_pos, ref, alt = split_variant(variant)

        chrom = ''
        pos = ''
        try:
            if ':' in chrom_pos:
                chrom_part, pos_part = chrom_pos.split(':', 1)
                chrom = chrom_part
                if pos_part.startswith('g.'):
                    pos = pos_part[2:]
                else:
                    pos = pos_part
        except ValueError:
            pass

        try:
            res = safe_getvariant(variant)
            variants_processed_in_file +=1
            
            if res is None or 'cgi' not in res:
                continue

            entry = {
                'input_variant': variant,
                'chrom': chrom,
                'Pos': pos,
                'ref': ref,
                'alt': alt
            }

            if isinstance(res['cgi'], list):
                for item in res['cgi']:
                    results.append({**entry, **item})
            elif isinstance(res['cgi'], dict):
                results.append({**entry, **res['cgi']})
        except Exception as e:
            print(f"Error processing variant {variant} from {os.path.basename(file_path)}: {e}. Skipping variant.")
            continue

    if not results:
        print(f"No variants with CGI data found or processed successfully in {os.path.basename(file_path)}.")
        file_end_time = time.time()
        duration_file = file_end_time - file_start_time
        print(f"Time taken for {os.path.basename(file_path)}: {duration_file:.2f} seconds (No data written).")
        return False

    result_df = pd.json_normalize(results)

    if not result_df.empty:
        result_df = result_df.drop_duplicates(ignore_index=True)
    
    if result_df.empty:
        print(f"No unique variants with CGI data to write for {os.path.basename(file_path)} after deduplication.")
        file_end_time = time.time()
        duration_file = file_end_time - file_start_time
        print(f"Time taken for {os.path.basename(file_path)}: {duration_file:.2f} seconds (No unique data to write).")
        return False

    try:
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            result_df.to_excel(writer, sheet_name='CGI_Annotated', index=False)
        print(f"CGI Annotated variants written to 'CGI_Annotated' sheet in {os.path.basename(file_path)}.")
        file_end_time = time.time()
        duration_file = file_end_time - file_start_time
        print(f"Time taken for {os.path.basename(file_path)}: {duration_file:.2f} seconds.")
        return True
    except Exception as e:
        print(f"Error writing to Excel file {os.path.basename(file_path)}: {e}")
        file_end_time = time.time()
        duration_file = file_end_time - file_start_time
        print(f"Time taken for {os.path.basename(file_path)} (with write error): {duration_file:.2f} seconds.")
        return False


def run_pipeline(args):
    overall_start_time = time.time() # Start timing for the entire operation

    if args.file_path:
        print(f"--- Starting single file annotation ---")
        process_file(args.file_path)
    elif args.folder_path:
        if not os.path.isdir(args.folder_path):
            print(f"Error: Folder not found: {args.folder_path}")
            return
        
        print(f"--- Starting folder annotation for: {args.folder_path} ---")
        folder_process_start_time = time.time()
        files_to_process = []
        for file_name in os.listdir(args.folder_path):
            if file_name.endswith(".xlsx"):
                files_to_process.append(os.path.join(args.folder_path, file_name))
        
        num_files_total = len(files_to_process)
        num_files_successfully_annotated = 0

        if num_files_total == 0:
            print("No .xlsx files found in the specified folder.")
        else:
            print(f"Found {num_files_total} .xlsx file(s) to process.")
            for i, full_file_path in enumerate(files_to_process):
                print(f"\nProcessing file {i+1} of {num_files_total}: {os.path.basename(full_file_path)}")
                if process_file(full_file_path):
                    num_files_successfully_annotated += 1
        
        folder_process_end_time = time.time()
        duration_folder_processing = folder_process_end_time - folder_process_start_time
        
        print(f"\n--- Folder Annotation Summary for '{args.folder_path}' ---")
        print(f"Total .xlsx files found: {num_files_total}")
        print(f"Files successfully processed/annotated: {num_files_successfully_annotated}")
        print(f"Total time taken for folder processing: {duration_folder_processing:.2f} seconds.")
        if num_files_total > 0:
            avg_time = duration_folder_processing / num_files_total if num_files_total > 0 else 0
            print(f"Average time per file (overall for folder): {avg_time:.2f} seconds.")

    overall_end_time = time.time()
    total_script_duration = overall_end_time - overall_start_time
    print(f"\n--- Total Script Execution Time: {total_script_duration:.2f} seconds ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Annotate Excel files (.xlsx) with CGI data from MyVariant.info. Provide either a single file using --file or a folder using --folder."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--file",
        dest="file_path", 
        help="Path to a single input Excel file (.xlsx)"
    )
    group.add_argument(
        "--folder",
        dest="folder_path", 
        help="Path to a folder containing .xlsx files to be processed"
    )
    
    args = parser.parse_args()
    run_pipeline(args)