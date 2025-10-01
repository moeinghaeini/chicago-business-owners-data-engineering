# Chicago Business Owners Data Engineering Project

## Dataset Overview

This project contains the **Chicago Business Owners** dataset, which provides comprehensive information about business owners and their associated businesses in Chicago. The dataset is sourced from the City of Chicago's open data portal and contains owner information for all accounts listed in the Business License Dataset.

## Dataset Details

- **Source**: [City of Chicago Data Portal](https://data.cityofchicago.org/d/ezma-pppn)
- **Data Owner**: Business Affairs & Consumer Protection
- **Time Period**: 2002 to present
- **Update Frequency**: Daily
- **Dataset Size**: 324,542 records (19MB)
- **Last Modified**: September 27, 2025

## Data Structure

The dataset contains **8 columns** with the following schema:

| Column | Description | Data Type |
|--------|-------------|-----------|
| Account Number | Unique identifier for the business account | Integer |
| Legal Name | Official legal name of the business | String |
| Owner First Name | First name of the business owner | String |
| Owner Middle Initial | Middle initial of the business owner | String |
| Owner Last Name | Last name of the business owner | String |
| Suffix | Name suffix (Jr., Sr., etc.) | String |
| Legal Entity Owner | Name of legal entity if owner is a corporation | String |
| Title | Owner's title/role in the business | String |

## Key Features

- **Multi-owner Support**: Businesses can have multiple owners with different roles
- **Legal Entity Tracking**: Distinguishes between individual and corporate owners
- **Role Classification**: Includes various business roles (CEO, President, Manager, etc.)
- **Historical Data**: Covers business ownership from 2002 to present

## Sample Data

```
Account Number: 85613
Legal Name: MERCER (US) LLC
Owner: Ron M Anderson
Title: OTHER

Account Number: 509748  
Legal Name: INVESTMENTS PERDOMO LLC
Owner: CELSO RAFAEL PERDOMO VARGAS
Title: MANAGING MEMBER
```

## Data Quality Notes

- Some records may have missing owner information (empty fields)
- Multiple owners per business are represented as separate rows
- Legal entity owners are tracked separately from individual owners
- Account numbers link to the main Business Licenses dataset

## Usage

This dataset is ideal for:
- Business ownership analysis
- Economic development research
- Business relationship mapping
- Demographic studies of business owners
- Compliance and regulatory analysis

## Related Datasets

- [Business Licenses Dataset](https://data.cityofchicago.org/dataset/Business-Licenses/r5kz-chrr) - Main business license information
- Use Account Number to link between datasets

## Files in this Repository

- `Business_Owners.csv` - Main dataset (324,542 records)
- `144ce8bf-b28c-4149-8f7a-7a098fe145e0.json` - Dataset metadata from Chicago Data Portal
- `readme.md` - This documentation file

## Data Engineering Considerations

- **Large Dataset**: 324K+ records require efficient processing
- **Data Cleaning**: Handle missing values and inconsistent formatting
- **Relationship Mapping**: Account numbers enable joins with other business datasets
- **Update Frequency**: Daily updates require automated data pipeline management