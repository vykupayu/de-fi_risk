# %%

from dune_client.client import DuneClient
import pandas as pd
import os
import matplotlib.pyplot as plt

os.chdir("C:/Users/vitko/Downloads")

# %%
# Initialize Dune client with your API key

dune = DuneClient("")
# %%
aave = dune.get_latest_result(5319271)
aave_df = pd.DataFrame(aave.result.rows)
# Select specific columns to keep
select_columns = ['evt_block_time', 'supply_apy', 
                  'variable_borrow_apy', 'stable_borrow_apy']
aave_df = aave_df[select_columns]
aave_df.to_csv("aave.csv", index=False)
if os.path.exists("aave.csv"):
    print("File aave.csv has been created successfully.")
# %%
# Read the CSV file to verify its contents
uni_v3 = dune.get_latest_result(5319481)
uni_v3_df = pd.DataFrame(uni_v3.result.rows)
select_columns = ['liquidity_usdc', 'liquidity_weth', 'liquidity_usd']
uni_v3_df.to_csv("uni_v3.csv", index=False)
if os.path.exists("uni_v3.csv"):
    print("File uni_v3.csv has been created successfully.")
# %%
uni_v2 = dune.get_latest_result(5319870)
uni_v2_df = pd.DataFrame(uni_v2.result.rows)
select_columns = ['total_liquidity']
uni_v2_df = uni_v2_df[select_columns]
uni_v2_df.to_csv("uni_v2.csv", index=False)
if os.path.exists("uni_v2.csv"):
    print("File uni_v2.csv has been created successfully.")
# %%
lido_staking = dune.get_latest_result(5319978)
lido_staking_df = pd.DataFrame(lido_staking.result.rows)
select_columns = ['Lido staking APR(instant)', 'Lido staking APR(ma_7)', 
                  'CL APR', 'EL APR']
lido_staking_df = lido_staking_df[select_columns]
lido_staking_df.to_csv("lido_staking.csv", index=False)
if os.path.exists("lido_staking.csv"):
    print("File lido_staking.csv has been created successfully.")
# %%
ETH_avg_daily_funding = dune.get_latest_result(5320024)
ETH_avg_daily_funding_df = pd.DataFrame(ETH_avg_daily_funding.result.rows)
select_columns = ['avg_funding_rate_geo']
ETH_avg_daily_funding_df = ETH_avg_daily_funding_df[select_columns]
ETH_avg_daily_funding_df.to_csv("ETH_avg_daily_funding.csv", index=False)
if os.path.exists("ETH_avg_daily_funding.csv"):
    print("File ETH_avg_daily_funding.csv has been created successfully.")
# %%
lido_WETH_STETH_liquidity = dune.get_latest_result(5319336)
lido_WETH_STETH_liquidity_df = pd.DataFrame(lido_WETH_STETH_liquidity.result.rows)
select_columns = ['weth_reserve', 'usdc_reserve', 'weth_price', 'usdc_price', 'trading_volume']
lido_WETH_STETH_liquidity_df = lido_WETH_STETH_liquidity_df[select_columns]
lido_WETH_STETH_liquidity_df.to_csv("lido_WETH_STETH_liquidity.csv", index=False)
if os.path.exists("lido_WETH_STETH_liquidity.csv"):
    print("File lido_WETH_STETH_liquidity.csv has been created successfully.")
# %%
price_data = dune.get_latest_result(5320039)
price_data_df = pd.DataFrame(price_data.result.rows)
select_columns = ['eth_usd', 'steth_usd']
price_data_df = price_data_df[select_columns]
price_data_df.to_csv("price_data.csv", index=False)
if os.path.exists("price_data.csv"):
    print("File price_data.csv has been created successfully.")
# %%
def check_file_exists(file_path):
    if os.path.exists(file_path):
        print(f"File {file_path} exists.")
    else:
        print(f"File {file_path} does not exist.")
    return os.path.exists(file_path)
# %%
def check_row_count(df, expected_count=1391):
    if len(df) == expected_count:
        print(f"DataFrame has the expected number of rows: {expected_count}")
    else:
        print(f"DataFrame has {len(df)} rows, expected {expected_count} rows.")
# %%
check_row_count(aave_df)
check_row_count(uni_v3_df)
check_row_count(uni_v2_df)
check_row_count(lido_staking_df)
check_row_count(ETH_avg_daily_funding_df)
check_row_count(lido_WETH_STETH_liquidity_df)
check_row_count(price_data_df)
# %%
plt.figure(figsize=(10, 5))
plt.plot(price_data_df['eth_usd'])
plt.title('ETH Price Over Time')
plt.xlabel('Index')
plt.ylabel('ETH/USD')
plt.grid(True)
plt.show()
# %%
# Merge all DataFrames on a common date index
# Ensure all DataFrames are sorted by date (if available)
# Use 'evt_block_time' as the datetime column if present, otherwise use index

# Convert evt_block_time to datetime for aave_df if present
if 'evt_block_time' in aave_df.columns:
    aave_df['evt_block_time'] = pd.to_datetime(aave_df['evt_block_time'])
    aave_df = aave_df.sort_values('evt_block_time').reset_index(drop=True)

# %%

# For other DataFrames, add a date index if not present
# Here, we assume all DataFrames have the same number of rows and are aligned by index

# Optionally, create a date range for the index
date_range = pd.date_range(start='2021-08-31', end='2025-06-21', periods=len(aave_df))
aave_df['date'] = date_range

# %
# Add the date column to all DataFrames
for df in [uni_v3_df, uni_v2_df, lido_staking_df, ETH_avg_daily_funding_df, lido_WETH_STETH_liquidity_df, price_data_df]:
    df['date'] = date_range

# Merge all DataFrames on 'date'
merged_df = aave_df.merge(uni_v3_df, on='date', how='left') \
                   .merge(uni_v2_df, on='date', how='left') \
                   .merge(lido_staking_df, on='date', how='left') \
                   .merge(ETH_avg_daily_funding_df, on='date', how='left') \
                   .merge(lido_WETH_STETH_liquidity_df, on='date', how='left') \
                   .merge(price_data_df, on='date', how='left')

# %%
# Drop all columns containing 'date' except 'evt_block_time'
cols_to_drop = [col for col in merged_df.columns if 'date' in col and col != 'evt_block_time']
merged_df = merged_df.drop(columns=cols_to_drop, errors='ignore')

# Drop the 'day' column if it exists
if 'day' in merged_df.columns:
    merged_df = merged_df.drop(columns=['day'])
    print("Column 'day' has been removed from the merged DataFrame.")

    # Drop the 'version' column if it exists
if 'version' in merged_df.columns:
    merged_df = merged_df.drop(columns=['version'])
    print("Column 'version' has been removed from the merged DataFrame.")


# Save the merged DataFrame to a CSV file
merged_df.to_csv("merged_data.csv", index=False)
print("Merged data without date columns saved to merged_data.csv")
# %%# Check if the merged DataFrame has the expected number of rows
expected_row_count = 1391  # Adjust this based on your expectations
if len(merged_df) == expected_row_count:
    print(f"Merged DataFrame has the expected number of rows: {expected_row_count}")
else:
    print(f"Merged DataFrame has {len(merged_df)} rows, expected {expected_row_count} rows.")
# %%# Check if the merged DataFrame has the expected columns
expected_columns = ['evt_block_time', 'supply_apy', 'variable_borrow_apy', 
                    'stable_borrow_apy', 'liquidity_usdc', 'liquidity_weth', 
                    'liquidity_usd', 'total_liquidity', 'Lido staking APR(instant)', 
                    'Lido staking APR(ma_7)', 'CL APR', 'EL APR', 
                    'avg_funding_rate_geo', 'weth_reserve', 'usdc_reserve', 
                    'weth_price', 'usdc_price', 'trading_volume', 'eth_usd', 'steth_usd']
if all(col in merged_df.columns for col in expected_columns):
    print("Merged DataFrame contains all expected columns.")
else:
    missing_columns = [col for col in expected_columns if col not in merged_df.columns]
    print(f"Merged DataFrame is missing the following expected columns: {missing_columns}")
# %%
