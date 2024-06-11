import os
import pandas as pd
from openpyxl import load_workbook
import openai
import re
import warnings
import tempfile
from google.cloud import storage

# Suppress warnings
warnings.filterwarnings('ignore')

# Set OpenAI API key
openai.api_key = os.getenv("OPENAI_API")

temp_dir = tempfile.gettempdir()

messages = [{"role": "system", "content": "You are a intelligent assistant."}]


def download_res1_csv():
    storage_client = storage.Client()
    bucket = storage_client.bucket('alex_uploads')
    blob = bucket.blob('knowledge_base/res1.csv')
    existing_csv_path = f'{temp_dir}/res1.csv'
    blob.download_to_filename(existing_csv_path)
    try:
        # Read existing CSV
        reference_df = pd.read_csv(existing_csv_path)
    except pd.errors.EmptyDataError:
        # If CSV is empty, create a new DataFrame
        reference_df = pd.DataFrame()
    print("Reference DataFrame:")
    print(reference_df)
    return reference_df


def suggest_column(filtered_columns, reference_df):
    message = f"""As a stock market and trading expert with a strong hold on annual report management, your task is to match template column names with source column names, based on provided input. Use the given reference data to assist with the matching.

The template columns are from the INPUTS-Financials sheet and are listed below:
template column names:['Travel Solutions Revenue',
'Hospitality Solutions Revenue',
'Eliminations',
'Revenue 4',
'Revenue 5',
'Revenue 6',
'Cost of products sold',
'Technology costs',
'COGS 3',
'COGS 4',
'COGS 5',
'COGS 6',
'COGS 7',
'COGS 8',
'COGS 9',
'Selling and administrative expenses',
'SG&A 2',
'SG&A 3',
'SG&A 4',
'SG&A 5',
'SG&A 6',
'SG&A 7',
'SG&A 8',
'Operating expenses',
'Above EBIT Expense 2',
'Above EBIT Expense 3',
'Above EBIT Expense 4',
'Above EBIT Expense 5',
'Above EBIT Expense 6',
'Above EBIT Expense 7',
'Above EBIT Expense 8',
'Interest expense',
'Other (income) expense, net',
'Loss on extinguishment of debt',
'Equity method income (loss)',
'Below EBIT Expense 4',
'Below EBIT Expense 5',
'Below EBIT Expense 6',
'Below EBIT Expense 7',
'Below EBIT Expense 8',
'Income Tax Expense / (Benefit)',
'(Loss) income from discontinued operations, net of tax',
'Below EBT Expense 2',
'Below EBT Expense 3',
'Below EBT Expense 4',
'Below EBT Expense 5',
'Below EBT Expense 6',
'Below EBT Expense 7',
'Restructuring and other costs',
'Other, net',
'Loss on extinguishment of debt2',
'Acquisition-related costs',
'Litigation costs, net',
'Stock-based compensation',
'Impairment and related charges',
'Amortization of upfront incentive consideration',
'Adjustment 10',
'Adjustment 11',
'Adjustment 12',
'Adjustment 13',
'Adjusted EBITDA',
'Covenant EBITDA']

source column names:{filtered_columns}

Rules:

1.Match template column names with source column names based on their meaning.
2.If no match is found, use 'NA' as the value.
3.Do not match multiple template columns to a single source column name.
4.Your reply should only contain the dictionary with template column names as keys and source column names as values. No other text should be included.
 Use the reference data: {reference_df} as a guide for matching."""

    # message = prompt
    if message:
        messages.append(
            {"role": "user", "content": message},
        )

        chat = openai.ChatCompletion.create(
            model='gpt-4', messages=messages
        )
        reply = chat.choices[0].message.content

        messages.clear()
        return reply


def suggest_col_names(filtered_columns, reference_df):
    message = f"""As a stock market and trading expert with a strong hold on annual report management, your task is to match template column names with source column names based on provided input. Use the given reference data to assist with the matching.

The template columns are from the Balance sheet and are listed below:
template column names:['Cash and cash equivalents',
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
'Redeemable Membership Units',
'Partners Equity',
'Additional Paid-In Capital',
'Retained Earnings / (Deficit)',
'Accumulated OCI',
'Noncontrolling interest',
'Treasury stock',
'Preferred stock',
'Shareholders Equity Other 5',
'Shareholders Equity Other 6']


source column names:{filtered_columns}

Rules:

1.Match template column names with source column names based on their meaning.
2.If no match is found, use 'NA' as the value.
3.Do not match multiple template columns to a single source column name.
4.Your reply should only contain the dictionary with template column names as keys and source column names as values. No other text should be included.
Use the reference data: {reference_df} as a guide for matching."""

    # message = prompt
    if message:
        messages.append(
            {"role": "user", "content": message},
        )

        chat = openai.ChatCompletion.create(
            model='gpt-4', messages=messages
        )
        reply = chat.choices[0].message.content

        messages.clear()
        return reply


def suggest_col_3(filtered_columns, reference_df):
    message = f"""As a stock market expert and trading expert with a strong hold on annual report management, your task is to match template column names with source column names based on provided input. Use the given reference data to assist with the matching.
The template columns are from the Cash Flow and are listed below:
template column names:['Net Income',
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
'Cash Impact of Exchange Rate',
'Cash used in discontinued operations',
'Unclassified Change in Cash 3',
'Unclassified Change in Cash 4',
'Cash and cash equivalents, beginning of period',
'Cash Interest',
'Cash taxes']

source column names:{filtered_columns}

Rules:

Match template column names with source column names based on their meaning.
If no match is found, use 'NA' as the value.
Do not match multiple template columns to a single source column name.
Your reply should only contain the dictionary with template column names as keys and source column names as values. No other text should be included.
Use the reference data: {reference_df} as a guide for matching."""

    # message = prompt
    if message:
        messages.append(
            {"role": "user", "content": message},
        )

        chat = openai.ChatCompletion.create(
            model='gpt-4', messages=messages
        )
        reply = chat.choices[0].message.content

        messages.clear()
        return reply


def drop_duplicate_columns_with_most_nans(df):
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


def column_mapping(source_path):
    reference_df = download_res1_csv()
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

    final_df.columns = final_df.columns.str.strip()

    final_df = drop_duplicate_columns_with_most_nans(final_df)
    messages.clear()

    filtered_columns = [col for col in columns if str(col).strip().lower() not in ['nan', 'nan ', 'fy']]

    reply_1 = suggest_column(filtered_columns, reference_df)
    reply_2 = re.findall(r'\{[^{}]+\}', reply_1)
    response_1 = reply_2[0].replace("\n", "").strip()
    response_1 = eval(response_1.replace("'", "\""))
    # print(response_1)
    reply_1 = suggest_col_names(filtered_columns, reference_df)
    reply_2 = re.findall(r'\{[^{}]+\}', reply_1)
    response_2 = reply_2[0].replace("\n", "").strip()
    response_2 = eval(response_2.replace("'", "\""))
    # print(response_2)
    reply_1 = suggest_col_3(filtered_columns, reference_df)
    reply_2 = re.findall(r'\{[^{}]+\}', reply_1)
    response_3 = reply_2[0].replace("\n", "").strip()
    response_3 = eval(response_3.replace("'", "\""))
    # print(response_3)
    column_mapping_dict = {**response_1, **response_2, **response_3}
    # print(column_mapping_dict)
    column_mapping_dict = process_dict(column_mapping_dict)
    return column_mapping_dict, filtered_columns


def process_excel_data(source_path):
    result_dict, source_columns = column_mapping(source_path)

    print(source_columns)
    print(result_dict)

    return result_dict, source_columns

