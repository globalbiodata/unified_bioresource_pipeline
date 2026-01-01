# Troubleshooting Guide

Common issues and solutions for the Unified Bioresource Pipeline.

---

## Google Colab Issues

### NumPy Error in Notebooks

**Symptoms:**
- Error message related to NumPy version mismatch
- Occurs in PyCaret or spaCy notebooks
- May mention `numpy.core` or version incompatibility

**Solution:**
Simply restart the notebook runtime and run all cells again.

1. Click **Runtime** → **Restart runtime**
2. Re-run all cells from the beginning

This resolves the NumPy version conflict that sometimes occurs when libraries are loaded.

**Affected Notebooks:**
- `pycaret_classification_colab.ipynb`
- `v2_ner_colab.ipynb` (when using spaCy)

---

### GPU Not Available

**Symptoms:**
- `torch.cuda.is_available()` returns False
- Training is extremely slow

**Solution:**
1. Go to **Runtime** → **Change runtime type**
2. Select **GPU** under Hardware accelerator
3. Click **Save**
4. Restart runtime and re-run

---

### Out of Memory (OOM) on Colab

**Symptoms:**
- CUDA out of memory error
- Kernel crashes during training/inference

**Solutions:**
1. Reduce batch size in the notebook
2. Use a smaller model or fewer papers
3. Upgrade to Colab Pro for more memory
4. Clear GPU memory:
   ```python
   import torch
   torch.cuda.empty_cache()
   ```

---

## URL Scanner Issues

### Scanner Gets Stuck / Hangs

**Symptoms:**
- URL scanner stops making progress
- Certain threads wait indefinitely for responses
- Usually happens when waiting for Internet Archive (Wayback Machine)

**Cause:**
Some archive.org requests take extremely long to respond, causing threads to hang.

**Solution:**
1. **Kill the process manually** (Ctrl+C)
2. The scanner **still produces usable output** up to the point it was killed
3. Use the partial output file to continue with subsequent phases
4. Optionally, use `--skip-wayback` flag to skip Wayback lookups:
   ```bash
   python scripts/phase9_finalization/24_check_urls_with_geo.py \
       --session-dir results/SESSION_ID \
       --skip-wayback
   ```

**Prevention:**
- Run scanning in smaller batches
- Use timeout settings in configuration
- Consider running overnight for large datasets

---

### URL Scanner Rate Limiting

**Symptoms:**
- HTTP 429 errors (Too Many Requests)
- Domains blocking requests

**Solution:**
Increase domain delay in configuration:
```yaml
url_scanning:
  domain_delay: 2.0  # Increase from 1.0 to 2.0 seconds
```

Or via command line:
```bash
python lib/url_scanner/scan_urls.py --domain-delay 2.0
```

---

## Git LFS Issues

### Models Show as Small Text Files

**Symptoms:**
- Model directories contain small (~130 byte) text files
- Files start with "version https://git-lfs.github.com/spec/v1"

**Solution:**
```bash
git lfs pull
# or
git lfs fetch --all
git lfs checkout
```

### LFS Bandwidth Exceeded

**Symptoms:**
- Error about LFS bandwidth quota

**Solution:**
- Wait for quota reset (monthly)
- Clone from a mirror without LFS tracking
- Download models separately and place in `models/` directory

---

## Dependency Issues

### PyCaret Conflicts

**Symptoms:**
- Import errors when using PyCaret
- Package version conflicts

**Solution:**
Use a separate virtual environment:
```bash
python -m venv pycaret_env
source pycaret_env/bin/activate
pip install pycaret[full]
```

### Transformers Version Mismatch

**Symptoms:**
- Model loading errors
- "Could not load model" messages

**Solution:**
```bash
pip install transformers==4.35.0
```

### spaCy Model Not Found

**Symptoms:**
- `OSError: [E050] Can't find model 'en_core_web_sm'`

**Solution:**
```bash
python -m spacy download en_core_web_sm
```

---

## Pipeline Issues

### Session Directory Not Found

**Symptoms:**
- Error: "Session directory not found"

**Solution:**
1. Check the session ID is correct:
   ```bash
   ls results/
   ```
2. Use full session ID including timestamp and suffix
3. Verify results directory path

### Missing Input Files

**Symptoms:**
- Error about missing CSV files in session input

**Solution:**
1. Ensure input files are in the correct directory
2. Check file names match expected patterns
3. Verify CSV format (columns, encoding)

### Phase Dependency Errors

**Symptoms:**
- Phase fails because previous phase output is missing

**Solution:**
1. Check which phases have run:
   ```bash
   ls results/SESSION_ID/
   ```
2. Re-run from the missing phase:
   ```bash
   python run_pipeline.py --session-id SESSION --start-phase N
   ```

---

## EPMC API Issues

### Rate Limiting

**Symptoms:**
- HTTP 429 errors from Europe PMC
- Metadata fetch fails

**Solution:**
Increase rate limit delay:
```python
# In script or config
rate_limit = 0.2  # Increase from 0.1
```

### API Timeouts

**Symptoms:**
- Requests timing out
- Partial results

**Solution:**
1. Check internet connection
2. Retry later (EPMC may be under maintenance)
3. Use checkpoint/resume feature to continue from where it stopped

---

## Memory Issues

### Out of Memory During Processing

**Symptoms:**
- Python killed by OOM killer
- MemoryError exceptions

**Solutions:**
1. Process in smaller batches
2. Use chunked reading for large CSVs:
   ```python
   for chunk in pd.read_csv('file.csv', chunksize=10000):
       process(chunk)
   ```
3. Increase system swap space
4. Use a machine with more RAM

---

## Common Error Messages

### "KeyError: 'pmid'"

**Cause:** Input CSV missing required column

**Solution:** Ensure input has `pmid` or `id` column with paper identifiers

### "ValueError: could not convert string to float"

**Cause:** Non-numeric data in numeric column

**Solution:** Clean input data, check for missing values

### "FileNotFoundError: [Errno 2] No such file or directory"

**Cause:** Path issue, often hardcoded paths

**Solution:**
1. Check file exists at specified path
2. Use `--session-dir` argument
3. Verify working directory

---

## Getting Help

If issues persist:

1. Check the execution log:
   ```bash
   cat results/SESSION_ID/execution_log.json
   ```

2. Run with verbose output:
   ```bash
   python run_pipeline.py --verbose --log-level DEBUG
   ```

3. Check individual script logs in session directory

4. Report issues at: https://github.com/globalbiodata/unified_bioresource_pipeline/issues
