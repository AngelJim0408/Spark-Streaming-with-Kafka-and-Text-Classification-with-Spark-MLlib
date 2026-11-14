"""
Helper script for Section 2.3b & 2.4: Computes accuracy on the 50 hand-labeled messages
(excluding 'None of these') and prints misclassified examples for the report.
"""

import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Evaluate 50 hand-labeled messages")
    parser.add_argument("--csv", required=True, help="Path to completed hand_labeled_50.csv")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    df["human_label"] = df["human_label"].astype(str).str.strip()
    df["predicted_class"] = df["predicted_class"].astype(str).str.strip()

    valid_labels = {"World", "Sports", "Business", "Sci/Tech", "None of these"}
    invalid_rows = df[~df["human_label"].isin(valid_labels)]
    if len(invalid_rows) > 0:
        print("ERROR: Found invalid or blank labels in human_label:")
        print(invalid_rows[["text", "human_label"]])
        return

    none_count = (df["human_label"] == "None of these").sum()
    in_domain = df[df["human_label"] != "None of these"]
    correct = (in_domain["human_label"] == in_domain["predicted_class"]).sum()
    total_in_domain = len(in_domain)
    accuracy = (correct / total_in_domain * 100.0) if total_in_domain > 0 else 0.0

    print("=== Section 2.3b Manual Evaluation Results ===")
    print(f"Total messages sampled: {len(df)}")
    print(f"Messages labeled 'None of these': {none_count}")
    print(f"Messages fitting one of the 4 AG News classes: {total_in_domain}")
    print(f"Correctly classified (among the 4 classes): {correct} / {total_in_domain} ({accuracy:.2f}%)")

    print("\n=== Section 2.4 Misclassified Examples (for Written Analysis) ===")
    wrong = in_domain[in_domain["human_label"] != in_domain["predicted_class"]]
    for idx, row in wrong.iterrows():
        print(f"- Text: {row['text']}")
        print(f"  Human Label: {row['human_label']} | Model Predicted: {row['predicted_class']} (Prob: {row['predicted_probability']:.4f})\n")


if __name__ == "__main__":
    main()
