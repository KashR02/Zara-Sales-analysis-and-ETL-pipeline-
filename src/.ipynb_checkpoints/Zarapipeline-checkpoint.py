import pandas as pd

def extract_data(path):
    df = pd.read_csv(path, sep=';')
    return df

def transform_data(df):
    df = df.dropna(subset=['name', 'description'])
    df['revenue'] = df['price'] * df['Sales Volume']
    df['price_segment'] = pd.cut(
        df['price'],
        bins=[0,50,100,200,500],
        labels=['budget','mid','premium','luxury']
    )
    return df

def load_data(df, output_path):
    df.to_csv(output_path, index=False)

def run_pipeline():
    raw_path = "data/raw/zara.csv"
    output_path = "data/processed/zara_cleaned.csv"

    df = extract_data(raw_path)
    df = transform_data(df)
    load_data(df, output_path)
    print("pipeline executed")


if __name__ == "__main__":
    run_pipeline()

