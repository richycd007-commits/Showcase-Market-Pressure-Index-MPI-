# Showcase-Market-Pressure-Index-MPI-
A data engineering project using Python and dbt to calculate fleet utilization risk via Z-score normalization of interest rates and oil prices.


# Market Pressure Index: Use Case Specification

## 1. Business Context
**Business Question:** "How do rising interest rates and fuel prices correlate to create a 'Risk Score' for our rental fleet utilization?"

**Objective:** The goal is to evaluate the macroeconomic headwinds affecting rental fleet utilization. Higher interest rates increase the cost of financing and operations, while higher crude oil prices directly impact transportation and fuel costs. Together, they constrain consumer and business spending, which poses a risk to fleet utilization rates.

**Outcome:** By creating a single, unified "Risk Score" or "Market Pressure Index" (MPI), the business can set threshold alerts, correlate the MPI against historical fleet utilization data, and adjust pricing or fleet size proactively.

---

## 2. Technical Specification

### 2.1 The Challenge
`Interest_Rate` (typically a small percentage, e.g., 5.25%) and `Crude_Oil_Price` (typically a larger dollar amount, e.g., $75.50) operate on entirely different scales. Simply adding them together would cause the oil price to dominate the metric. 

### 2.2 The Solution: Z-Score Normalization
To combine these two metrics meaningfully into a single index, we must standardize them using **Z-score normalization**. This converts both metrics into a common scale (standard deviations from their respective historical means). 

### 2.3 SQL Formula Definition

The formula calculates the standard score for both metrics and applies a weighting to each. For this baseline model, we assume equal weighting (50/50).

**Mathematical Formula:**
```text
Market_Pressure_Index = (W1 * Z(Interest_Rate)) + (W2 * Z(Crude_Oil_Price))
```
Where `Z(x) = (x - mean) / standard_deviation`

**SQL Implementation (Snowflake / PostgreSQL / BigQuery compatible):**
```sql
WITH historical_stats AS (
    -- 1. Calculate the historical mean and standard deviation for both metrics
    SELECT 
        AVG(Interest_Rate) AS avg_ir,
        STDDEV(Interest_Rate) AS std_ir,
        AVG(Crude_Oil_Price) AS avg_oil,
        STDDEV(Crude_Oil_Price) AS std_oil
    FROM 
        raw_market_data
),

normalized_data AS (
    -- 2. Normalize the current row values using the historical stats
    SELECT 
        d.date,
        d.Interest_Rate,
        d.Crude_Oil_Price,
        
        -- Interest Risk component
        (d.Interest_Rate - s.avg_ir) / NULLIF(s.std_ir, 0) AS z_score_interest,
        
        -- Fuel Risk component
        (d.Crude_Oil_Price - s.avg_oil) / NULLIF(s.std_oil, 0) AS z_score_oil
        
    FROM 
        raw_market_data d
    CROSS JOIN 
        historical_stats s
)

-- 3. Calculate the final Market Pressure Index
SELECT 
    date,
    Interest_Rate,
    Crude_Oil_Price,
    
    -- Combine into a single index (using 50/50 weighting here)
    -- A higher positive MPI indicates greater market pressure/risk.
    (0.5 * z_score_interest) + (0.5 * z_score_oil) AS Market_Pressure_Index

FROM 
    normalized_data
ORDER BY 
    date DESC;
```

### 2.4 Interpretation & Next Steps
- **MPI > 0**: Market pressure is above average (higher risk to utilization).
- **MPI < 0**: Market pressure is below average (lower risk to utilization).
- **Next Step - Integration**: This SQL model will be materialized as a view/table in the intermediate or mart layer of the data warehouse. It can then be joined directly with internal "Fleet Utilization" fact tables to analyze the historical correlation (e.g., calculating Pearson's r between `Market_Pressure_Index` and `Utilization_Rate`).
