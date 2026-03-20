import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os

def generate_correlation_matrix():
    # 1. Generate realistic mock time-series data
    # (In a real scenario, this would be passed from your data warehouse or CSV)
    np.random.seed(42)
    dates = pd.date_range(start='2019-01-01', periods=60, freq='M')
    
    # Base economic trend
    trend = np.linspace(0, 10, 60)
    
    # Simulate variables with realistic correlations
    crude_oil = 50 + (trend * 3) + np.random.normal(0, 5, 60)
    interest_rate = 1.5 + (trend * 0.6) + np.random.normal(0, 0.4, 60)
    sp_volatility = 15 + (trend * 0.8) + np.random.normal(0, 4, 60)
    
    df = pd.DataFrame({
        'Date': dates,
        'Crude_Oil_Price': crude_oil,
        'Interest_Rate': interest_rate,
        'S&P_500_Volatility': sp_volatility
    }).set_index('Date')

    # 2. Calculate the Pearson correlation matrix
    corr_matrix = df.corr()

    # 3. Design a professional Heatmap
    plt.figure(figsize=(8, 6))
    
    # Use a clean, professional aesthetic
    sns.set_theme(style="white")
    
    # Diverging color palette (Blue to Red, representing negative to positive)
    cmap = sns.diverging_palette(230, 20, as_cmap=True)

    # Draw the heatmap
    ax = sns.heatmap(corr_matrix, 
                annot=True,            # Show the correlation numbers
                fmt=".2f",             # Format numbers to 2 decimal places
                cmap=cmap,             # Color map
                vmin=-1, vmax=1,       # Standardize scale from -1 to 1
                center=0,
                cbar_kws={"shrink": .8}, 
                linewidths=1.5,        # Add spacing between grid cells
                annot_kws={"size": 12, "weight": "bold"})
    
    # Polish the formatting
    plt.title('Macro-Economic Factors vs. Market Volatility\n(Construction Risk Drivers)', 
              fontsize=14, weight='bold', pad=20)
    
    # Clean up axis labels
    plt.xticks(rotation=15, ha='right', fontsize=10)
    plt.yticks(rotation=0, fontsize=10)
    plt.xlabel('')
    plt.ylabel('')
    
    plt.tight_layout()

    # 4. Save the plot to the local directory
    output_path = os.path.join(os.path.dirname(__file__), 'correlation_heatmap.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Heatmap successfully generated and saved to: {output_path}")

if __name__ == "__main__":
    generate_correlation_matrix()
