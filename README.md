# 🧬 CGI Annotation Script

[![Python](https://img.shields.io/badge/Python-3.7%2B-blue.svg)](https://www.python.org/)
[![MyVariant.info](https://img.shields.io/badge/API-MyVariant.info-green.svg)](http://myvariant.info/)

This Python script processes `.xlsx` Excel files with genomic variant data and annotates them using **[MyVariant.info](http://myvariant.info/)**, extracting **Cancer Genome Interpreter (CGI)** annotations. It supports both single-file and batch folder processing.

---

## 📁 Input Format

The input should be a `.xlsx` file with a column titled Genomic Alteration containing genomic data (e.g., chr1:g.162745497A>T)

## 📄Install dependencies using **Anaconda**, **Command Prompt**, or **PowerShell**:
pip install -r requirements.txt

## 🚀Usage:
Run the script from the terminal

✅ Annotate a single Excel file:
python CGI_Annotation_Script.py --file /path/to/input_file.xlsx

📂 Annotate all Excel files in a folder:
python CGI_Annotation_Script.py --folder /path/to/folder/

## 📤 Output
A new sheet named CGI_Annotated is appended as result which essentially contains information about input variant,	chrom position, ref, alt, association, cdna, drug, evidence_level, gene, protein_change
