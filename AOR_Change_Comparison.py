import pandas as pd
from io import StringIO

# Complete dataset
data = """
Country,Origin Date,Holiday,cohort,status,Pre_10W,Pre_5W,Post_5W,Post_10W,Delta_5W,Delta_10W
BE,2025-03-31,Not a Holiday,Cohort A (0),running,5.62,3.14,3.00,5.35,0.14,0.27
BE,2025-03-31,Not a Holiday,Cohort B (-1),running,5.71,3.14,3.08,5.27,0.06,0.44
BE,2025-03-31,Not a Holiday,Cohort C (+1),running,5.59,3.10,2.92,5.21,0.18,0.38
BE,2025-03-31,Not a Holiday,Cohort E (+2),running,5.20,2.93,2.69,4.88,0.24,0.32
DE,2024-05-20,Whit Monday,Cohort A (0),running,4.56,2.77,2.67,4.32,0.10,0.24
DE,2024-05-20,Whit Monday,Cohort C (+1),running,4.73,2.92,2.77,4.42,0.15,0.31
DE,2024-05-20,Whit Monday,Cohort D (-2),running,4.70,2.91,2.69,4.36,0.22,0.34
DE,2024-05-20,Whit Monday,Cohort E (+2),running,4.90,2.97,2.60,4.40,0.37,0.50
DE,2024-10-03,German Unity Day,Cohort A (0),running,3.86,2.51,2.41,3.95,0.10,-0.09
DE,2024-10-03,German Unity Day,Cohort B (-1),running,3.95,2.74,2.69,4.49,0.05,-0.54
DE,2024-10-03,German Unity Day,Cohort C (+1),running,3.82,2.49,2.59,4.31,-0.10,-0.49
DE,2025-01-02,Not a Holiday,Cohort A (0),running,4.60,2.84,2.86,4.74,-0.02,-0.14
DE,2025-01-02,Not a Holiday,Cohort C (+1),running,3.90,2.50,2.56,4.01,-0.06,-0.11
DE,2025-01-02,Not a Holiday,Cohort D (-2),running,4.06,2.58,2.53,4.15,0.05,-0.09
DE,2025-01-02,Not a Holiday,Cohort G (-3),running,3.99,2.53,2.49,4.02,0.04,-0.03
GB,2024-04-01,Easter Monday,Cohort A (0),running,4.79,2.90,2.40,4.40,0.50,0.39
GB,2024-04-01,Easter Monday,Cohort C (+1),running,4.91,2.94,2.47,4.49,0.47,0.42
GB,2024-04-01,Easter Monday,Cohort E (+2),running,4.72,2.85,2.30,4.23,0.55,0.49
GB,2024-05-27,Spring Bank Holiday,Cohort A (0),running,4.66,2.87,2.80,4.49,0.07,0.17
GB,2024-05-27,Spring Bank Holiday,Cohort B (-1),running,4.54,2.79,2.73,4.35,0.06,0.19
GB,2024-05-27,Spring Bank Holiday,Cohort C (+1),running,4.68,2.88,2.83,4.51,0.05,0.17
GB,2024-08-26,Summer Bank,Cohort A (0),running,4.67,2.79,2.82,4.60,-0.03,0.07
GB,2024-08-26,Summer Bank,Cohort B (-1),running,4.62,2.77,2.75,4.47,0.02,0.15
GB,2024-08-26,Summer Bank,Cohort C (+1),running,4.70,2.80,2.83,4.62,-0.03,0.08
GB,2024-08-26,Summer Bank,Cohort E (+2),running,4.85,2.69,2.76,4.89,-0.07,-0.04
GB,2024-12-23,Not a Holiday,Cohort A (0),running,5.04,2.95,2.46,4.39,0.49,0.65
GB,2024-12-23,Not a Holiday,Cohort D (-2),running,5.09,2.96,2.46,4.39,0.50,0.70
GB,2024-12-31,New Year’s Eve,Cohort A (0),running,4.75,2.90,2.40,4.20,0.50,0.55
GB,2024-12-31,New Year’s Eve,Cohort B (-1),running,4.70,2.86,2.36,4.15,0.50,0.55
GB,2024-12-31,New Year’s Eve,Cohort D (-2),running,4.72,2.87,2.33,4.09,0.54,0.63
GB,2024-12-31,New Year’s Eve,Cohort E (+2),running,4.63,2.83,2.33,4.05,0.50,0.58
FR,2024-04-01,Easter Monday,Cohort A (0),running,4.42,2.72,2.58,4.24,0.14,0.18
FR,2024-04-01,Easter Monday,Cohort C (+1),running,4.18,2.60,2.56,4.17,0.04,0.01
FR,2024-04-01,Easter Monday,Cohort E (+2),running,3.96,2.47,2.38,3.88,0.09,0.08
FR,2024-04-01,Easter Monday,Cohort F (+3),running,3.80,2.41,2.25,3.65,0.16,0.15
FR,2025-01-02,Not a Holiday,Cohort A (0),running,5.19,2.98,2.47,3.99,0.51,1.20
FR,2025-01-02,Not a Holiday,Cohort C (+1),running,5.28,3.02,2.45,3.88,0.57,1.40
FR,2025-04-21,Easter Monday,Cohort A (0),running,6.52,3.65,2.73,4.60,0.92,1.92
FR,2025-04-21,Easter Monday,Cohort C (+1),running,6.45,3.60,2.64,4.31,0.96,2.14
FR,2025-04-21,Easter Monday,Cohort E (+2),running,6.36,3.56,2.47,4.00,1.09,2.36
FR,2025-04-21,Easter Monday,Cohort F (+3),running,6.20,3.53,2.32,3.71,1.21,2.49
"""

# Load into DataFrame
df = pd.read_csv(StringIO(data))

# Extract cohort letter
df['Cohort_Label'] = df['cohort'].str.extract(r'Cohort (\w)')

# Group by country & holiday
grouped = df.groupby(['Country', 'Holiday'])

# Initialize counters
total_comparisons = 0
a_better_or_equal = 0

# Compare A vs others in group
for (_, _), group in grouped:
    if 'A' not in group['Cohort_Label'].values:
        continue
    delta_a = group[group['Cohort_Label'] == 'A']['Delta_5W'].values[0]
    for _, row in group.iterrows():
        if row['Cohort_Label'] != 'A':
            total_comparisons += 1
            if delta_a <= row['Delta_5W']:
                a_better_or_equal += 1



# --- Δ AOR (10W) Comparison ---

# Initialize new counters
total_comparisons_10w = 0
a_better_or_equal_10w = 0

# Repeat comparison logic for Δ AOR 10W
for (_, _), group in grouped:
    if 'A' not in group['Cohort_Label'].values:
        continue
    delta_a_10w = group[group['Cohort_Label'] == 'A']['Delta_10W'].values[0]
    for _, row in group.iterrows():
        if row['Cohort_Label'] != 'A':
            total_comparisons_10w += 1
            if delta_a_10w <= row['Delta_10W']:
                a_better_or_equal_10w += 1

# Calculate result
percentage_10w = (a_better_or_equal_10w / total_comparisons_10w) * 100 if total_comparisons_10w else 0

# Print result
print(f"Cohort A performed better or equal in {a_better_or_equal_10w} out of {total_comparisons_10w} comparisons — approximately {percentage_10w:.1f}% based on Δ AOR (10W).")


# Result
percentage = (a_better_or_equal / total_comparisons) * 100 if total_comparisons else 0
print(f"Cohort A performed better or equal in {a_better_or_equal} out of {total_comparisons} comparisons — approximately {percentage:.1f}%.")


print(f"""
--- Comparison Summary ---

Δ AOR (5W):
Cohort A performed better or equal in {a_better_or_equal} out of {total_comparisons} comparisons
→ Approx. {percentage:.1f}% of the time

Δ AOR (10W):
Cohort A performed better or equal in {a_better_or_equal_10w} out of {total_comparisons_10w} comparisons
→ Approx. {percentage_10w:.1f}% of the time
""")
