import os
import argparse
import logging
import fitz  # PyMuPDF
import pymupdf4llm

# ------------------------------------------------------------
# 🪵 Logger Configuration
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# 🛠 Argument Parser
# ------------------------------------------------------------
parser = argparse.ArgumentParser(description="Extract PDFs to Markdown TXT")
parser.add_argument(
    "--source_path",
    required=True,
    help="Root folder containing source PDFs"
)
parser.add_argument(
    "--target_path",
    required=True,
    help="Root folder to save extracted TXT files"
)
args = parser.parse_args()

input_root = args.source_path
output_root = args.target_path

logger.info(f"Starting PDF extraction from source: {input_root}")

# ------------------------------------------------------------
# 📝 PDF Processing Function
# ------------------------------------------------------------
def process_pdf(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        text = pymupdf4llm.to_markdown(doc)
        doc.close()

        relative_path = os.path.relpath(pdf_path, input_root)
        relative_noext = os.path.splitext(relative_path)[0]
        output_dir = os.path.join(output_root, os.path.dirname(relative_path))
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_root, relative_noext + ".txt")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(text)

        logger.info(f"✅ Processed {pdf_path} → {output_file}")
        return True

    except Exception as e:
        logger.error(f"❌ Error processing {pdf_path}: {e}")
        return False

# ------------------------------------------------------------
# 🚶 Walk all PDFs under input_root
# ------------------------------------------------------------
processed_count = 0
for root, _, files in os.walk(input_root):
    for file in files:
        if file.lower().endswith(".pdf"):
            pdf_path = os.path.join(root, file)
            if process_pdf(pdf_path):
                processed_count += 1

if processed_count == 0:
    logger.error("No PDF files were processed. Exiting.")
    raise SystemExit(1)

logger.info(f"Finished processing {processed_count} PDF file(s).")
