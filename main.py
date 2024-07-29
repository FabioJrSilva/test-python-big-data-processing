from src.sales_processor import SalesProcessor

if __name__ == "__main__":
    processor = SalesProcessor(file_path='data/vendas.csv')
    processor.process_sales_data()
    processor.print_summary()