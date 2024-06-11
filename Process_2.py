import os
import pandas as pd
from openpyxl import load_workbook
import openai
import re
import warnings
import tempfile

temp_dir = tempfile.gettempdir()


def drop_duplicate_columns_with_most_nans(df):
    """
    Among duplicate columns, keep only the one with the least NaNs and drop others.

    Parameters:
    - df: input DataFrame

    Returns:
    - DataFrame with desired columns removed based on criterion.
    """
    # Convert DataFrame to a dictionary for better handling of duplicate keys
    df_dict = df.to_dict(orient='list')
    processed = set()
    result_dict = {}

    for col, values in df_dict.items():
        if col not in processed:
            # Extract all columns with the same name
            duplicate_data = {k: v for k, v in df_dict.items() if str(k) == str(col)}

            # Check if duplicate_data has more than one column
            if len(duplicate_data) > 1:
                # Determine which column has the least NaNs
                min_nans_col = min(duplicate_data, key=lambda k: sum(1 for x in duplicate_data[k] if pd.isna(x)))
            else:
                min_nans_col = col  # Use the current column if there are no duplicates

            result_dict[min_nans_col] = df_dict[min_nans_col]
            processed.add(col)

    # Convert the resulting dictionary back to a DataFrame
    result_df = pd.DataFrame(result_dict)
    return result_df


def sort_dataframe_columns(df):
    # Convert column names to strings and then extract the numbers
    column_numbers = {col: re.search(r'\d+', str(col)) for col in df.columns}

    # If the column doesn't have a number, we'll set it to a very low value to ensure it comes first in the sorting
    for col, match in column_numbers.items():
        column_numbers[col] = int(match.group()) if match else -1

    # Sort columns by their extracted number
    sorted_columns = sorted(df.columns, key=lambda x: column_numbers[x])

    return df[sorted_columns]


def process_dict(d):
    # Create a reverse mapping of values to keys
    reverse_mapping = {}
    for key, value in d.items():
        value = value.strip()
        if value not in reverse_mapping:
            reverse_mapping[value] = []
        reverse_mapping[value].append(key)

    # Identify keys that should be changed to "NA"
    keys_to_change = []
    for value, keys in reverse_mapping.items():
        if len(keys) > 1 and value != "NA":
            keys_to_change.extend(keys[1:])

    # Update the original dictionary
    for key in keys_to_change:
        d[key] = "NA"

    return d


def column_mapping(source_path, res_map):
    df = pd.read_excel(source_path, sheet_name=None)
    sheets = [i for i in df.keys()]

    all_dfs = []
    for sheet in sheets:
        # df_temp = df[sheet][4:]
        df_temp = df[sheet]
        df_temp.columns = df_temp.iloc[0].str.replace(' ', '').str.strip('')
        df_temp = sort_dataframe_columns(df_temp)
        # df_temp = df_temp[1:]
        # df_temp = df_temp.fillna(0)
        df_temp = df_temp.T
        df_temp.columns = df_temp.iloc[0]
        df_temp = df_temp[1:]
        df_temp['FY'] = df_temp.index
        all_dfs.append(df_temp)

    final_df = all_dfs[0]
    for df_temp in all_dfs[1:]:
        final_df = final_df.join(df_temp, on='FY', how='outer', lsuffix='', rsuffix=' ')

    cols_atlast = final_df.index
    columns = []
    for column in final_df.columns:
        columns.append(column)
    filtered_columns = [col for col in columns if str(col).strip().lower() not in ['nan', 'nan ', 'fy']]
    final_df.columns = final_df.columns.str.strip()

    final_df = drop_duplicate_columns_with_most_nans(final_df)
    column_list = filtered_columns
    final_df.to_csv(f'{temp_dir}/pre-pivot.csv')
    res1 = {}
    count = 1
    for key, value in res_map.items():
        if value == 'NA':
            res1[f'NA{count}'] = key.strip()
            count = count + 1
        else:
            res1[value.strip()] = key.strip()

    # print(res1)
    df_full = final_df
    # df_full = drop_duplicate_columns_with_most_nans(df_full)
    df_full.columns = df_full.columns.str.strip()
    df_full1 = df_full.rename(columns=res1)

    for i in res_map.keys():
        if res_map[i].strip() == 'NA':
            df_full1[i] = ''

    # nan_counts = df_full1.isna().sum()
    # for col_name in df_full1.columns:
    #       if df_full1.columns[df_full1.columns == col_name].size > 1:
    #           # Get the duplicate columns
    #           duplicate_cols = df_full1[[col_name]]

    #           # Determine which of the duplicate columns has the least NaNs
    #           min_nans_col = nan_counts.loc[duplicate_cols.columns].idxmin()

    #           # Get the columns to drop (all duplicates except the one with the least NaNs)
    #           cols_to_drop = [col for col in df_full1.columns if col in duplicate_cols.columns and col != min_nans_col]

    #           # Drop the unwanted columns
    #           df_full1 = df_full1.drop(columns=cols_to_drop)

    #           # Update the nan_counts to reflect the dropped columns
    #           nan_counts = nan_counts.drop(labels=cols_to_drop)

    df_filtered = df_full1[res_map.keys()]
    df_filtered = df_filtered.apply(pd.to_numeric, errors='coerce')
    df_filtered = df_filtered.astype(float)

    columns_to_set = ['Actual Line Item',
                      'Income Statement',
                      'Net sales',
                      'Total Cost of goods sold',
                      'Gross profit',
                      'Total Selling and administrative expenses',
                      'Operating income',
                      'Income / (Loss) before income taxes',
                      'Net Income / (Loss)',
                      'Net Income / (Loss)2',
                      'Interest expense2',
                      'Income Tax Expense / (Benefit)2',
                      'EBIT',
                      'Depreciation and amortization2',
                      'Other amortization2',
                      'Depreciation & Amortization',
                      'Analyst reconcile check',
                      'EBITDA',
                      'CHECK',
                      '(Loss) income from discontinued operations, net of tax2',
                      'Total EBITDA Adjustments',
                      'Analyst reconcile check2',
                      'CHECK2',
                      'Balance Sheet',
                      'Total current assets',
                      'Total assets',
                      'Total current liabilities',
                      'Total liabilities',
                      'Total shareholders equity',
                      'Total liabilities and shareholders equity',
                      'CHECK3',
                      'Cash Flow',
                      'CHECK4',
                      'Net Working Capital Increase / (Decrease)',
                      'Total adjustments',
                      'Net cash (used in) / provided by operating activities',
                      'Net cash (used in) / provided by investing activities',
                      'Net cash (used in) / provided by financing activities',
                      'Increase / (Decrease) in cash & equiv.',
                      'Cash and cash equivalents, end of period',
                      'CHECK5']

    for col in columns_to_set:
        df_filtered.loc[:len(df_filtered) - 1, col] = ''

    header_columns = ['Company Name:', 'Financials as of', 'FYE', 'Currency', 'Adjusted EBITDA Build',
                      'EBITDA Adjustments:', 'Assets', 'Current assets', 'Liabilities and Shareholders Equity',
                      'Current liabilities', "Shareholders Equity", 'Cash flows from operating activities',
                      'Reconciliation of net income/(loss) to cash:',
                      'Net Working Capital Increase / (Decrease) in cash:', 'Cash flows from investing activities',
                      'Cash flows from financing activities']

    header_indices = [1, 2, 3, 4, 5, 6, 14, 15, 16, 17, 18, 19, 20, 21, 23, 25]

    for header, index in zip(header_columns, header_indices):
        df_filtered.insert(index, header, '')

    # start_fy = 'Y 2022'

    # fy_values = [f'Y {year}' for year in range(int(start_fy.split()[-1]), 2006, -1)]

    # df_filtered['Actual Line Item'] = df_filtered['Actual Line Item'].replace('', '0').astype(float)
    df_filtered['Income Statement'] = df_filtered['Income Statement'].replace('', '0').astype(float)
    df_filtered['Net sales'] = df_filtered['Net sales'].replace('', '0').astype(float)
    df_filtered['Total Cost of goods sold'] = df_filtered['Total Cost of goods sold'].replace('', '0').astype(float)
    df_filtered['Gross profit'] = df_filtered['Gross profit'].replace('', '0').astype(float)
    df_filtered['Total Selling and administrative expenses'] = df_filtered[
        'Total Selling and administrative expenses'].replace('', '0').astype(float)
    df_filtered['Operating income'] = df_filtered['Operating income'].replace('', '0').astype(float)
    df_filtered['Income / (Loss) before income taxes'] = df_filtered['Income / (Loss) before income taxes'].replace('',
                                                                                                                    '0').astype(
        float)
    df_filtered['Net Income / (Loss)'] = df_filtered['Net Income / (Loss)'].replace('', '0').astype(float)
    df_filtered['Net Income / (Loss)2'] = df_filtered['Net Income / (Loss)2'].replace('', '0').astype(float)
    df_filtered['Interest expense2'] = df_filtered['Interest expense2'].replace('', '0').astype(float)
    df_filtered['Income Tax Expense / (Benefit)2'] = df_filtered['Income Tax Expense / (Benefit)2'].replace('',
                                                                                                            '0').astype(
        float)
    df_filtered['EBIT'] = df_filtered['EBIT'].replace('', '0').astype(float)
    df_filtered['Depreciation and amortization2'] = df_filtered['Depreciation and amortization2'].replace('',
                                                                                                          '0').astype(
        float)
    df_filtered['Other amortization2'] = df_filtered['Other amortization2'].replace('', '0').astype(float)
    df_filtered['Depreciation & Amortization'] = df_filtered['Depreciation & Amortization'].replace('', '0').astype(
        float)
    df_filtered['Analyst reconcile check'] = df_filtered['Analyst reconcile check'].replace('', '0').astype(float)
    df_filtered['EBITDA'] = df_filtered['EBITDA'].replace('', '0').astype(float)
    df_filtered['CHECK'] = df_filtered['CHECK'].replace('', '0').astype(float)
    df_filtered['(Loss) income from discontinued operations, net of tax2'] = df_filtered[
        '(Loss) income from discontinued operations, net of tax2'].replace('', '0').astype(float)
    df_filtered['Total EBITDA Adjustments'] = df_filtered['Total EBITDA Adjustments'].replace('', '0').astype(float)
    df_filtered['Analyst reconcile check2'] = df_filtered['Analyst reconcile check2'].replace('', '0').astype(float)
    df_filtered['CHECK2'] = df_filtered['CHECK2'].replace('', '0').astype(float)
    df_filtered['Balance Sheet'] = df_filtered['Balance Sheet'].replace('', '0').astype(float)
    df_filtered['Total current assets'] = df_filtered['Total current assets'].replace('', '0').astype(float)
    df_filtered['Total assets'] = df_filtered['Total assets'].replace('', '0').astype(float)
    df_filtered['Total current liabilities'] = df_filtered['Total current liabilities'].replace('', '0').astype(float)
    df_filtered['Total liabilities'] = df_filtered['Total liabilities'].replace('', '0').astype(float)
    df_filtered['Total shareholders equity'] = df_filtered['Total shareholders equity'].replace('', '0').astype(float)
    df_filtered['Total liabilities and shareholders equity'] = df_filtered[
        'Total liabilities and shareholders equity'].replace('', '0').astype(float)
    df_filtered['CHECK3'] = df_filtered['CHECK3'].replace('', '0').astype(float)
    df_filtered['Cash Flow'] = df_filtered['Cash Flow'].replace('', '0').astype(float)
    df_filtered['CHECK4'] = df_filtered['CHECK4'].replace('', '0').astype(float)
    df_filtered['Net Working Capital Increase / (Decrease)'] = df_filtered[
        'Net Working Capital Increase / (Decrease)'].replace('', '0').astype(float)
    df_filtered['Total adjustments'] = df_filtered['Total adjustments'].replace('', '0').astype(float)
    df_filtered['Net cash (used in) / provided by operating activities'] = df_filtered[
        'Net cash (used in) / provided by operating activities'].replace('', '0').astype(float)
    df_filtered['Net cash (used in) / provided by investing activities'] = df_filtered[
        'Net cash (used in) / provided by investing activities'].replace('', '0').astype(float)
    df_filtered['Net cash (used in) / provided by financing activities'] = df_filtered[
        'Net cash (used in) / provided by financing activities'].replace('', '0').astype(float)
    df_filtered['Increase / (Decrease) in cash & equiv.'] = df_filtered[
        'Increase / (Decrease) in cash & equiv.'].replace('', '0').astype(float)
    df_filtered['Cash and cash equivalents, end of period'] = df_filtered[
        'Cash and cash equivalents, end of period'].replace('', '0').astype(float)
    df_filtered['CHECK5'] = df_filtered['CHECK5'].replace('', '0').astype(float)

    # df_filtered['Actual Line Item'] = df_filtered['Actual Line Item'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Income Statement'] = df_filtered['Income Statement'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Net sales'] = df_filtered['Net sales'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total Cost of goods sold'] = df_filtered['Total Cost of goods sold'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Gross profit'] = df_filtered['Gross profit'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total Selling and administrative expenses'] = df_filtered[
        'Total Selling and administrative expenses'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Operating income'] = df_filtered['Operating income'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Income / (Loss) before income taxes'] = df_filtered['Income / (Loss) before income taxes'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Net Income / (Loss)'] = df_filtered['Net Income / (Loss)'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Net Income / (Loss)2'] = df_filtered['Net Income / (Loss)2'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Interest expense2'] = df_filtered['Interest expense2'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Income Tax Expense / (Benefit)2'] = df_filtered['Income Tax Expense / (Benefit)2'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['EBIT'] = df_filtered['EBIT'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Depreciation and amortization2'] = df_filtered['Depreciation and amortization2'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Other amortization2'] = df_filtered['Other amortization2'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Depreciation & Amortization'] = df_filtered['Depreciation & Amortization'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Analyst reconcile check'] = df_filtered['Analyst reconcile check'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['EBITDA'] = df_filtered['EBITDA'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['CHECK'] = df_filtered['CHECK'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['(Loss) income from discontinued operations, net of tax2'] = df_filtered[
        '(Loss) income from discontinued operations, net of tax2'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total EBITDA Adjustments'] = df_filtered['Total EBITDA Adjustments'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Analyst reconcile check2'] = df_filtered['Analyst reconcile check2'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['CHECK2'] = df_filtered['CHECK2'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Balance Sheet'] = df_filtered['Balance Sheet'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total current assets'] = df_filtered['Total current assets'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total assets'] = df_filtered['Total assets'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total current liabilities'] = df_filtered['Total current liabilities'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total liabilities'] = df_filtered['Total liabilities'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total shareholders equity'] = df_filtered['Total shareholders equity'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total liabilities and shareholders equity'] = df_filtered[
        'Total liabilities and shareholders equity'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['CHECK3'] = df_filtered['CHECK3'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Cash Flow'] = df_filtered['Cash Flow'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['CHECK4'] = df_filtered['CHECK4'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Net Working Capital Increase / (Decrease)'] = df_filtered[
        'Net Working Capital Increase / (Decrease)'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Total adjustments'] = df_filtered['Total adjustments'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Net cash (used in) / provided by operating activities'] = df_filtered[
        'Net cash (used in) / provided by operating activities'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Net cash (used in) / provided by investing activities'] = df_filtered[
        'Net cash (used in) / provided by investing activities'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Net cash (used in) / provided by financing activities'] = df_filtered[
        'Net cash (used in) / provided by financing activities'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Cash and cash equivalents, end of period'] = df_filtered[
        'Cash and cash equivalents, end of period'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Increase / (Decrease) in cash & equiv.'] = df_filtered['Increase / (Decrease) in cash & equiv.'].apply(
        lambda x: -abs(x) if x != 0 else 0)
    df_filtered['Cash and cash equivalents, end of period'] = df_filtered[
        'Cash and cash equivalents, end of period'].apply(lambda x: -abs(x) if x != 0 else 0)
    df_filtered['CHECK5'] = df_filtered['CHECK5'].apply(lambda x: -abs(x) if x != 0 else 0)

    df_filtered.insert(0, 'FY', cols_atlast)

    order1 = ['Company Name:',
              'Financials as of',
              'FYE',
              'Currency',
              'FY',
              'Income Statement',
              'Travel Solutions Revenue',
              'Hospitality Solutions Revenue',
              'Eliminations',
              'Revenue 4',
              'Revenue 5',
              'Revenue 6',
              'Net sales',
              'Cost of products sold',
              'Technology costs',
              'COGS 3',
              'COGS 4',
              'COGS 5',
              'COGS 6',
              'COGS 7',
              'COGS 8',
              'COGS 9',
              'Total Cost of goods sold',
              'Gross profit',
              'Selling and administrative expenses',
              'SG&A 2',
              'SG&A 3',
              'SG&A 4',
              'SG&A 5',
              'SG&A 6',
              'SG&A 7',
              'SG&A 8',
              'Total Selling and administrative expenses',
              'Operating expenses',
              'Above EBIT Expense 2',
              'Above EBIT Expense 3',
              'Above EBIT Expense 4',
              'Above EBIT Expense 5',
              'Above EBIT Expense 6',
              'Above EBIT Expense 7',
              'Above EBIT Expense 8',
              'Operating income',
              'Interest expense',
              'Other (income) expense, net',
              'Loss on extinguishment of debt',
              'Equity method income (loss)',
              'Below EBIT Expense 4',
              'Below EBIT Expense 5',
              'Below EBIT Expense 6',
              'Below EBIT Expense 7',
              'Below EBIT Expense 8',
              'Income / (Loss) before income taxes',
              'Income Tax Expense / (Benefit)',
              '(Loss) income from discontinued operations, net of tax',
              'Below EBT Expense 2',
              'Below EBT Expense 3',
              'Below EBT Expense 4',
              'Below EBT Expense 5',
              'Below EBT Expense 6',
              'Below EBT Expense 7',
              'Net Income / (Loss)',
              'Adjusted EBITDA Build',
              'Net Income / (Loss)2',
              'Interest expense2',
              'Income Tax Expense / (Benefit)2',
              'EBIT',
              'Depreciation and amortization2',
              'Other amortization2',
              'Depreciation & Amortization',
              'Analyst reconcile check',
              'EBITDA',
              'CHECK',
              'EBITDA Adjustments:',
              'Restructuring and other costs',
              'Other, net',
              'Loss on extinguishment of debt2',
              'Acquisition-related costs',
              'Litigation costs, net',
              'Stock-based compensation',
              '(Loss) income from discontinued operations, net of tax2',
              'Impairment and related charges',
              'Amortization of upfront incentive consideration',
              'Adjustment 10',
              'Adjustment 11',
              'Adjustment 12',
              'Adjustment 13',
              'Total EBITDA Adjustments',
              'Analyst reconcile check2',
              'Adjusted EBITDA',
              'CHECK2',
              'Covenant EBITDA',
              'Balance Sheet',
              'Assets',
              'Current assets',
              'Cash and cash equivalents',
              'Restricted Cash',
              'Other Cash Equivalent 1',
              'Other Cash Equivalent 2',
              'Accounts Receivable - net',
              'Inventories - net',
              'Prepaid expenses and other current assets',
              'Current assets held for sale',
              'Other Current Assets 3',
              'Other Current Assets 4',
              'Other Current Assets 5',
              'Other Current Assets 6',
              'Other Current Assets 7',
              'Total current assets',
              'PP&E, net',
              'Intangible Assets',
              'Goodwill',
              'Other non current assets',
              'Equity method investments',
              'Deferred income taxes',
              'Long-term assets held for sale',
              'Other Assets 6',
              'Other Assets 7',
              'Other Assets 8',
              'Other Assets 9',
              'Other Assets 10',
              'Total assets',
              'Liabilities and Shareholders Equity',
              'Current liabilities',
              'Accounts payable',
              'Current portion LTD',
              'RLOC borrowings',
              'Accrued compensation and related benefits',
              'Accrued subscriber incentives',
              'Deferred revenues',
              'Other accrued liabilities',
              'Tax Receivable Agreement',
              'Current liabilities held for sale',
              'Other Current Liability 9',
              'Other Current Liability 10',
              'Other Current Liability 11',
              'Other Current Liability 12',
              'Total current liabilities',
              'Long-term debt',
              'Long-term Debt 2',
              'Long-term Debt 3',
              'Long-term Debt 4',
              'Finance Leases',
              'Deferred income taxes2',
              'Other noncurrent liabilities',
              'Long-term liabilities held for sale',
              'Other Liability 3',
              'Other Liability 4',
              'Other Liability 5',
              'Other Liability 6',
              'Other Liability 7',
              'Other Liability 8',
              'Other Liability 9',
              'Other Liability 10',
              'Total liabilities',
              'Shareholders Equity',
              'Redeemable Membership Units',
              'Partners Equity',
              'Additional Paid-In Capital',
              'Retained Earnings / (Deficit)',
              'Accumulated OCI',
              'Noncontrolling interest',
              'Treasury stock',
              'Preferred stock',
              'Shareholders Equity Other 5',
              'Shareholders Equity Other 6',
              'Total shareholders equity',
              'Total liabilities and shareholders equity',
              'CHECK3',
              'Cash Flow',
              'Cash flows from operating activities',
              'Net Income',
              'CHECK4',
              'Reconciliation of net income/(loss) to cash:',
              'Depreciation and amortization',
              'Other amortization',
              'Amortization of deferred loan costs',
              'Gain on sale of assets and investments',
              'Stock-based compensation expense',
              'Loss on fair value of investment',
              'Deferred income taxes3',
              'Pension settlement charge',
              'Impairment and related charges2',
              'Debt modification costs',
              'Loss on extinguishment of debt3',
              'Gain on loan converted to equity',
              'Loss (income) from discontinued operations',
              'Other',
              'Provision for expected credit losses',
              'Acquisition termination fee',
              'Facilities-related charges',
              'Amortization of upfront incentive consideration2',
              'Dividends received from equity method investments',
              'Paid-in-kind interest',
              'Other Operating Adjustment 18',
              'Other Operating Adjustment 19',
              'Other Operating Adjustment 20',
              'Net Working Capital Increase / (Decrease) in cash:',
              'Accounts receivable, net',
              'Inventories, net',
              'Prepaid expenses and other current assets2',
              'Other Current Asset 2',
              'Other Current Asset 3',
              'Other Current Asset 4',
              'Other Current Asset 5',
              'Other Current Asset 6',
              'Other Current Asset 7',
              'Other Current Asset 8',
              'Other noncurrent assets',
              'Capitalized implementation costs',
              'Upfront incentive consideration',
              'Other Noncurrent Asset 4',
              'Other Noncurrent Asset 5',
              'Accounts payable2',
              'Accrued compensation and related benefits2',
              'Deferred revenue including upfront solution fees',
              'Other Current Liability 3',
              'Other Current Liability 4',
              'Other Current Liability 5',
              'Other Current Liability 6',
              'Other Current Liability 7',
              'Other Current Liability 8',
              'Other long-term liabilities',
              'Other Noncurrent Liability 2',
              'Net Working Capital Increase / (Decrease)',
              'Total adjustments',
              'Net cash (used in) / provided by operating activities',
              'Cash flows from investing activities',
              'Proceeds from the sale of property and equipment',
              'Capital expenditures',
              'Purchase of investment in equity securities',
              'Other investing activities',
              'Acquisitions, net of cash acquired',
              'Other Investing Activity 4',
              'Other Investing Activity 5',
              'Other Investing Activity 6',
              'Other Investing Activity 7',
              'Other Investing Activity 8',
              'Other Investing Activity 9',
              'Other Investing Activity 10',
              'Other Investing Activity 11',
              'Other Investing Activity 12',
              'Net cash (used in) / provided by investing activities',
              'Cash flows from financing activities',
              'Proceeds from revolving credit facility',
              'Repayment of revolving credit facility',
              'Dividends paid to Parent',
              'Payments on borrowings from lenders',
              'Proceeds of borrowings from lenders',
              'Debt discount and issuance costs',
              'Net payment on the settlement of equity-based awards',
              'Other financing activities',
              'Payment for settlement of exchangeable notes',
              'Proceeds from issuance of preferred stock, net',
              'Proceeds from issuance of common stock, net',
              'Payments on Tax Receivable Agreement',
              'Repurchase of common stock',
              'Proceeds from sale of redeemable shares in subsidiary',
              'Proceeds from borrowings under AR Facility',
              'Payments on borrowings under AR Facility',
              'Financing Activity 15',
              'Financing Activity 16',
              'Financing Activity 17',
              'Net cash (used in) / provided by financing activities',
              'Cash Impact of Exchange Rate',
              'Cash used in discontinued operations',
              'Unclassified Change in Cash 3',
              'Unclassified Change in Cash 4',
              'Increase / (Decrease) in cash & equiv.',
              'Cash and cash equivalents, beginning of period',
              'Cash and cash equivalents, end of period',
              'CHECK5',
              'Cash Interest',
              'Cash taxes']

    df_filtered = df_filtered[order1]

    df_filtered = df_filtered.rename(columns={'FY': 'Actual Line Item'})

    df_filtered = drop_duplicate_columns_with_most_nans(df_filtered)

    df_filtered.fillna(0, inplace=True)

    transposed_df = df_filtered.T

    transposed_df.to_excel(f'{temp_dir}/output.xlsx', index=True)

    return column_list, transposed_df, df_filtered


def generate_excel(source_path, res_map):
    column_list, mapped, df123 = column_mapping(source_path, res_map)
    mapped = mapped.reset_index()
    new_columns = [chr(65 + i) for i in range(mapped.shape[1])]

    column_mapping_dict = dict(zip(mapped.columns, new_columns))
    mapped = mapped.rename(columns=column_mapping_dict)
    colmap = mapped.columns

    row_index = 5

    for idx, col in enumerate(mapped.columns[1:], start=1):
        excel_col = chr(66 + idx)
        formula = f'=IFERROR(TEXT({excel_col}6, "0") & " (FYE)", "")'
        mapped.at[row_index, col] = formula

    row_index = 12

    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(SUM({excel_col1}8:{excel_col1}13),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 22
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(SUM({excel_col1}15:{excel_col1}23),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 23
    for idx, col in enumerate(mapped.columns):
        if mapped[col].notnull().any():
            excel_col = chr(66 + idx)
            excel_col1 = chr(66 + idx + 1)
            formula = f'=IFERROR(+{excel_col1}14-{excel_col1}24,"-")'
            mapped.at[row_index, excel_col] = formula

    row_index = 32
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(SUM({excel_col1}26:{excel_col1}33),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 41
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}25 - SUM({excel_col1}34:{excel_col1}42), "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 51
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}43-{excel_col1}44 - SUM({excel_col1}45:{excel_col1}52), "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 60
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}53 - SUM({excel_col1}54:{excel_col1}61), "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 62
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}62, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 63
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR({excel_col1}44, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 64
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}54, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 65
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(sum({excel_col1}64:{excel_col1}66),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 66
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}175, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 67
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}176, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 68
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+sum({excel_col1}68:{excel_col1}69),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 69
    for idx, col in enumerate(mapped.columns):
        if mapped[col].notnull().any():
            excel_col = chr(66 + idx)
            excel_col1 = chr(66 + idx + 1)
            formula = f'=IFERROR({excel_col1}67+{excel_col1}70,"-")'
            mapped.at[row_index, excel_col] = formula

    row_index = 70
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR({excel_col1}71, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 71
    for idx, col in enumerate(mapped.columns[1:], start=0):  # Start from the second column (B in Excel)
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(IF(ABS(SUM({excel_col1}67:{excel_col1}69)-{excel_col1}72)<3, "OK", SUM({excel_col1}67:{excel_col1}69)-{excel_col1}72),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 79
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR({excel_col1}55, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 86
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}75:{excel_col1}87),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 87
    for idx, col in enumerate(mapped.columns):
        if mapped[col].notnull().any():
            excel_col = chr(66 + idx)
            excel_col1 = chr(66 + idx + 1)
            formula = f'=IFERROR(+{excel_col1}71+{excel_col1}88,"-")'
            mapped.at[row_index, excel_col] = formula

    row_index = 89
    for idx, col in enumerate(mapped.columns[1:], start=0):  # Start from the second column (B in Excel)
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(IF(ABS({excel_col1}88+{excel_col1}72-{excel_col1}90)<3, "OK", {excel_col1}88+{excel_col1}72-{excel_col1}90),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 91
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}7, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 107
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}96:{excel_col1}108),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 120
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}109:{excel_col1}121),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 136
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}125:{excel_col1}137),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 153
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}138:{excel_col1}154),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 165
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}157:{excel_col1}166),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 166
    for idx, col in enumerate(mapped.columns):
        if mapped[col].notnull().any():
            excel_col = chr(66 + idx)
            excel_col1 = chr(66 + idx + 1)
            formula = f'=IFERROR(+{excel_col1}167+{excel_col1}155,"-")'
            mapped.at[row_index, excel_col] = formula

    row_index = 167
    for idx, col in enumerate(mapped.columns[1:], start=0):  # Start from the second column (B in Excel)
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(IF(ABS({excel_col1}168-{excel_col1}122)<3, "OK", {excel_col1}168-{excel_col1}122),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 168
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}7, "-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 171
    for idx, col in enumerate(mapped.columns[1:], start=0):  # Start from the second column (B in Excel)
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(IF(ABS({excel_col1}172-{excel_col1}62)<3, "OK", {excel_col1}62),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 223
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}199:{excel_col1}208)+Sum(+{excel_col1}214:{excel_col1}222),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 224
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}175:{excel_col1}224),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 225
    for idx, col in enumerate(mapped.columns):
        if mapped[col].notnull().any():
            excel_col = chr(66 + idx)
            excel_col1 = chr(66 + idx + 1)
            formula = f'=IFERROR(+{excel_col1}226+{excel_col1}172,"-")'
            mapped.at[row_index, excel_col] = formula

    row_index = 241
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}229:{excel_col1}242),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 262
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(Sum({excel_col1}245:{excel_col1}263),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 267
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(+{excel_col1}227+{excel_col1}243+{excel_col1}264+Sum({excel_col1}265:{excel_col1}268),"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 269
    for idx, col in enumerate(mapped.columns):
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR({excel_col1}96+{excel_col1}97,"-")'
        mapped.at[row_index, excel_col] = formula

    row_index = 270
    for idx, col in enumerate(mapped.columns[1:], start=0):  # Start from the second column (B in Excel)
        excel_col = chr(66 + idx)
        excel_col1 = chr(66 + idx + 1)
        formula = f'=IFERROR(IF(ABS({excel_col1}271-{excel_col1}270-{excel_col1}269)<3, "OK", {excel_col1}271-{excel_col1}270-{excel_col1}269),"-")'
        mapped.at[row_index, excel_col] = formula

    mapped = mapped[colmap]
    return mapped



