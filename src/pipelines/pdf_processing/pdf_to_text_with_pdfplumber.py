import os
import argparse
import pdfplumber

# ------------------------------------------------------------
# 🛠 Argument Parser
# ------------------------------------------------------------
parser = argparse.ArgumentParser(description="Extract PDFs to TXT using pdfplumber")
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

# ------------------------------------------------------------
# 📝 PDF Processing Function
# ------------------------------------------------------------
def process_pdf(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"

        # Build output path keeping same structure
        relative_path = os.path.relpath(pdf_path, input_root)
        relative_noext = os.path.splitext(relative_path)[0]  # drop .pdf
        output_dir = os.path.join(output_root, os.path.dirname(relative_path))
        os.makedirs(output_dir, exist_ok=True)

        # Save full text as one file
        output_file = os.path.join(output_root, relative_noext + ".txt")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(text)

        print(f"✅ Processed {pdf_path} → {output_file}")

    except Exception as e:
        print(f"❌ Error processing {pdf_path}: {e}")


# ------------------------------------------------------------
# 🚶 Walk all PDFs under input_root
# ------------------------------------------------------------
for root, _, files in os.walk(input_root):
    for file in files:
        if file.lower().endswith(".pdf"):
            pdf_path = os.path.join(root, file)
            process_pdf(pdf_path)
