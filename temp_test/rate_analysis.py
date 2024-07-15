
import pandas as pd
import matplotlib.pyplot as plt

# Define the data
data = {
    "Data Size": ["1MB", "1MB", "5MB", "5MB", "10MB", "10MB", "20MB", "20MB"],
    "Attempt": [1, 2, 1, 2, 1, 2, 1, 2],
    "Total Time (min)": [2, 2, 7, 7, 12+7/60, 12+9/60, 22+55/60, 22+54/60],
    "Total Records": [7209, 7209, 34541, 34541, 69081, 69081, 138161, 138161],
    "SplitFile (s)": [2.793, 2.653, 10.935, 11.112, 21.243, 21.988, 25.307, 24.059],
    "DataExtraction (s)": [9.639, 8.277, 32.973, 25.468, 52.641, 53.755, 74.297, 72.114],
    "Transformation (s)": [20.054, 7.608, 75.51, 41.842, 95.671, 96.067, 122.449, 118.576],
    "LoadData (s)": [77.057, 237.689, 1247.458, 664.776, 1630.364, 2306.376, 3791.356, 3778.055],
    "BackupData (s)": [28.315, 38.831, 24.754, 21.118, 49.872, 49.712, 62.264, 53.178]
}

# Create a DataFrame
df = pd.DataFrame(data)

# Convert the total time to seconds for more detailed analysis
df["Total Time (s)"] = df["Total Time (min)"] * 60

# Plotting the data
fig, axes = plt.subplots(2, 1, figsize=(12, 16))

# Total execution time for each data size
axes[0].plot(df["Data Size"], df["Total Time (min)"], marker='o')
axes[0].set_title('Total Execution Time by Data Size')
axes[0].set_xlabel('Data Size')
axes[0].set_ylabel('Total Time (min)')
axes[0].grid(True)

# Execution time by stages for each data size
stages = ["SplitFile (s)", "DataExtraction (s)", "Transformation (s)", "LoadData (s)", "BackupData (s)"]
for stage in stages:
    axes[1].plot(df["Data Size"], df[stage], marker='o', label=stage)
axes[1].set_title('Execution Time by Stages and Data Size')
axes[1].set_xlabel('Data Size')
axes[1].set_ylabel('Time (s)')
axes[1].legend()
axes[1].grid(True)

plt.tight_layout()
plt.show()