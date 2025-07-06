# Kognitos ROI Benchmark Demo

## Objective
This project provides a self-contained, command-line demo that proves the Return on Investment (ROI) of using Kognitos for a common business workflow: Accounts Payable processing.

It directly compares a simulated manual "baseline" process against a Kognitos-automated process, providing clear, quantifiable metrics on performance, cost, and accuracy.

## What it Demonstrates
* **Cycle Time Reduction:** Measures the dramatic speed increase of automation.
* **Cost Savings:** Calculates the financial impact using a conservative cost model.
* **Error Rate Improvement:** Shows the reduction in costly human errors.
* **Audit & Governance:** Proves the existence of a tamper-proof, cryptographic audit trail for every automated run—a key requirement for regulated industries.

## Prerequisites
* Python 3.12+
* Poetry (https://python-poetry.org/)

## How to Run
From the root directory of the project, execute one command:

```bash
make demo
```
This single command will handle everything: clean previous runs, install dependencies, generate synthetic data (including messy, real-world examples), run the full benchmark, and print the final report to your console.

## Expected Output
The script will output a summary table directly to your terminal, similar to this:

| Metric              | Baseline   | Kognitos   | Delta      |
|---------------------|------------|------------|------------|
| Avg Cycle Time (s)  | 1.90       | 0.10       | -94.77%    |
| Avg Cost ($)        | 0.0237     | 0.0010     | -95.73%    |
| Error Rate (%)      | 16.00      | 6.00       | -62.50%    |
| Total Runs          | 50         | 50         |            |
| Successful Runs     | 42         | 47         |            |
