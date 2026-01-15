import os, csv, time, requests, sys, signal
from pathlib import Path


# URL = "http://localhost/raven-import-and-submit-api/upload-csv-file"
URL = "https://raven.dev.heat.icl.gtri.org/raven-import-api/upload-csv-file"
AUTH = ("client","secret")
TYPE_FIELD = {"type":"mdi"}

SOURCE_CSV = Path("../results/MILWAUKEE_TO_RAVEN_2025-12-16.csv")
OUT_DIR = Path("../results/split_upload_chunks")

# Rows Per Chunk
# I recommend something from 100-500 depending on your machine, 1000 can work but crashes sometimes
CHUNK_SIZE = 400
MAX_RETRIES = 3
RETRY_BACKOFF = 60.0
LAST_SUCCESS_UPLOAD = 56

OUT_DIR.mkdir(parents=True, exist_ok=True) 

def split_csv(source_csv: Path, out_dir: Path, chunk_size: int) -> list[Path]:
    chunk_paths = []
    out_f = None
    
    try:
        with source_csv.open("r", newline="", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            
            # Save header from CSV
            header = next(reader)

            file_index = 1
            row_count = 0

            # Temporary path to store each chunk
            out_path = out_dir / f"{source_csv.stem}_part_{file_index:03d}.csv"
            out_f = out_path.open("w", newline="", encoding="utf-8")
            writer = csv.writer(out_f)
            writer.writerow(header)

            for row in reader:
                writer.writerow(row)
                row_count += 1
                
                if row_count >= chunk_size:
                    out_f.close()
                    chunk_paths.append(out_path)
                    
                    file_index += 1
                    row_count = 0
                    out_path = out_dir / f"{source_csv.stem}_part_{file_index:03d}.csv"
                    out_f = out_path.open("w", newline="", encoding="utf-8")
                    writer = csv.writer(out_f)
                    writer.writerow(header)
            
            out_f.close()
            chunk_paths.append(out_path)
    except Exception as e:
        if out_f:
            out_f.close()
        # Cleanup any partial chunks on error
        for chunk_path in chunk_paths:
            if chunk_path.exists():
                chunk_path.unlink()
        raise e
    
    return chunk_paths


def upload_file(path: Path) -> tuple[int,str]:
    with path.open("rb") as f:
        files = {"file": (path.name, f, "text/csv")}
        r = requests.post(URL, auth=AUTH, files=files, data=TYPE_FIELD, timeout=6000)
        return r.status_code, r.text

# Upload a file with some retry attempts, return true if successful, false if otherwise
def upload_with_retry(path: Path) -> bool:
    for attempt in range(1, MAX_RETRIES+1):
        try:
            code, text = upload_file(path)
            if 200 <= code < 300:
                print(f"[OK] {path.name} -> {code}")
                return True
            else:
                print(f"[SERVER ERR] {path.name} -> {code} | {text}")
        except requests.RequestException as e:
            print(f"[Net Err] {path.name} attempt {attempt}/{MAX_RETRIES}: {e}")
        
        if (attempt < MAX_RETRIES):
            sleep_s = RETRY_BACKOFF ** attempt
            time.sleep(sleep_s)

    print(f"[FAILED] {path.name} after {MAX_RETRIES} attempts")
    return False

# Delete chunk csv's from results/split_upload_chunks folder
def cleanup_chunks(chunk_paths: list[Path]):
    for chunk_path in chunk_paths:
        if chunk_path.exists():
            chunk_path.unlink()
            print(f"[CLEANUP] Removed {chunk_path.name}")

# Handle keyboard interrupt
def signal_handler(sig, frame):
    print('\n[INTERRUPTED] Stopping upload process...')
    if 'chunk_paths' in globals() and chunk_paths:
        print("Cleaning up chunks...")
        cleanup_chunks(chunk_paths)
    sys.exit(0)

# Set up signal handler for proper shutdown
signal.signal(signal.SIGINT, signal_handler)

# ********* Main Driver *****************
chunk_paths = []
try:
    # Validate source file exists
    if not SOURCE_CSV.exists():
        print(f"Error: Source CSV file not found: {SOURCE_CSV}")
        sys.exit(1)
    
    print("Splitting CSV...")

    chunk_paths = split_csv(SOURCE_CSV, OUT_DIR, CHUNK_SIZE)
    print(f"Created {len(chunk_paths)} chunks in {OUT_DIR.resolve()}")

    print("Uploading chunks...")
    successful_uploads = 0
    
    for i, chunk_path in enumerate(chunk_paths[LAST_SUCCESS_UPLOAD:], start=LAST_SUCCESS_UPLOAD+1):
        print(f"Uploading chunk {i}/{len(chunk_paths)}: {chunk_path.name}")
        
        # Timer per upload
        t0 = time.perf_counter()
        ok = upload_with_retry(chunk_path)
        dt = time.perf_counter() - t0

        if not ok:
            print(f"\n[ERROR] Upload failed for {chunk_path.name}")
            print(f"Stopping upload process. {successful_uploads}/{len(chunk_paths)} chunks uploaded successfully.")
            print("Cleaning up remaining chunks...")
            cleanup_chunks(chunk_paths)
            sys.exit(1)
        
        successful_uploads += 1
        print(f"[TIMING] completed in {dt/60:.2f} minutes")
    
    print(f"\n[SUCCESS] All {len(chunk_paths)} chunks uploaded successfully!")
    # Clean up all chunks after successful upload
    cleanup_chunks(chunk_paths)
        
except Exception as e:
    print(f"Script Error: {e}")
    if chunk_paths:
        print("Cleaning up partial chunks...")
        cleanup_chunks(chunk_paths)
    sys.exit(1)
finally:
    # Delete the temporary split_upload_chunks folder from results
    OUT_DIR.rmdir()
