import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Update the path to the staging data
STAGING_DATA_PATH = "data/staging_data"

# Data
data = {
    'Study': ['AWV022', 'AWV021', 'AWV032', 'AWV002', 'AWV039'],
    'Unanimous Agreement': [77.46, 36.28, 67.01, 79.14, 81.69],
    'Two Agree': [20.89, 53.99, 31.16, 18.88, 17.57],
    'No Agreement': [1.64, 9.72, 1.83, 1.98, 0.74]
}

df = pd.DataFrame(data)

# Set up the matplotlib figure
plt.figure(figsize=(14, 8))
plt.style.use('ggplot')

# Create the stacked bar plot
bottom_bars = np.zeros(5)

for category in ['Unanimous Agreement', 'Two Agree', 'No Agreement']:
    plt.bar(df['Study'], df[category], bottom=bottom_bars, label=category)
    bottom_bars += df[category]

# Customize the plot
plt.title('Sleep Scoring Agreement Analysis Across Multiple Studies', fontsize=16, pad=20)
plt.xlabel('Study', fontsize=12)
plt.ylabel('Percentage', fontsize=12)

# Add a legend
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

# Add percentage labels on the bars
for i, study in enumerate(df['Study']):
    bottom = 0
    for category in ['Unanimous Agreement', 'Two Agree', 'No Agreement']:
        height = df.loc[i, category]
        plt.text(i, bottom + height/2, f'{height:.1f}%', ha='center', va='center')
        bottom += height

# Adjust layout and display
plt.tight_layout()
plt.savefig('sleep_scoring_agreement_analysis.png', dpi=300, bbox_inches='tight')
plt.close()

print("Visualization saved as 'sleep_scoring_agreement_analysis.png'")