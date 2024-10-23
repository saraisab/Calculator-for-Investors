from sqlalchemy import create_engine, Column, Float, String, desc, func
from sqlalchemy.orm import sessionmaker, declarative_base
import csv


engine = create_engine('sqlite:///investor.db')
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

class Companies(Base):
    __tablename__ = 'companies'

    ticker = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)


class Financial(Base):
    __tablename__ = 'financial'

    ticker = Column(String, primary_key=True)
    ebitda = Column(Float)
    sales = Column(Float)
    net_profit = Column(Float)
    market_price = Column(Float)
    net_debt = Column(Float)
    assets = Column(Float)
    equity = Column(Float)
    cash_equivalents = Column(Float)
    liabilities = Column(Float)


def insert_companies(ticker_val, name_val, sector_val):
    company_insert = Companies(ticker=ticker_val, name=name_val, sector=sector_val)
    session.add(company_insert)
    session.commit()


def insert_financial(ticker_val, ebitda_val, sales_val, net_profit_val, market_price_val, net_debt_val, assets_val,
                     equity_val, cash_equivalents_val, liabilities_val):
    financial_insert = Financial(ticker=ticker_val, ebitda=ebitda_val, sales=sales_val, net_profit=net_profit_val,
                                 market_price=market_price_val, net_debt=net_debt_val, assets=assets_val,
                                 equity=equity_val, cash_equivalents=cash_equivalents_val, liabilities=liabilities_val)
    session.add(financial_insert)
    session.commit()


def read_data_companies():
    with open('test/companies.csv') as companies_file:
        file_reader_comp = csv.DictReader(companies_file, delimiter=',')
        for line in file_reader_comp:
            insert_companies(line['ticker'], line['name'], line['sector'])


def read_data_financial():
    lista = ['ticker', 'ebitda', 'sales', 'net_profit', 'market_price', 'net_debt', 'assets', 'equity',
             'cash_equivalents', 'liabilities']

    with open('test/financial.csv') as financial_file:
        file_reader_financial = csv.DictReader(financial_file, delimiter=',')
        for line in file_reader_financial:
            cont = 0
            for value in line.values():
                if len(value) == 0:
                    line[lista[cont]] = None
                cont += 1

            insert_financial(line['ticker'], line['ebitda'], line['sales'], line['net_profit'], line['market_price'],
                             line['net_debt'], line['assets'], line['equity'], line['cash_equivalents'],
                             line['liabilities'])


def enter_companies():
    ticker_new = input("Enter ticker (in the format 'MOON'): \n")
    company_name_new = input("Enter company (in the format 'Moon Corp'):\n")
    industry_name_new = input("Enter industries (in the format 'Technology'):\n")
    return ticker_new, company_name_new, industry_name_new


def enter_financial():
    ebitda_new = float(input("Enter ebitda (in the format '987654321'):\n"))
    sales_new = float(input("Enter sales (in the format '987654321'):\n"))
    profit_new = float(input("Enter net profit (in the format '987654321'):\n"))
    price_new = float(input("Enter market price (in the format '987654321'):\n"))
    net_new = float(input("Enter net debt (in the format '987654321'):\n"))
    assets_new = float(input("Enter assets (in the format '987654321'):\n"))
    equity_new = float(input("Enter equity (in the format '987654321'):\n"))
    cash_new = float(input("Enter cash equivalents (in the format '987654321'):\n"))
    liabilities_new = float(input("Enter liabilities (in the format '987654321'):\n"))
    return ebitda_new, sales_new, profit_new, price_new, net_new, assets_new, equity_new, cash_new, liabilities_new


def insert_company_new():
    ticker_new, company_name_new, industry_name_new = enter_companies()
    ebitda_new, sales_new, profit_new, price_new, net_new, assets_new, equity_new, cash_new, liabilities_new = enter_financial()
    insert_companies(ticker_new, company_name_new, industry_name_new)
    insert_financial(ticker_new, ebitda_new, sales_new, profit_new, price_new, net_new, assets_new, equity_new, cash_new
                     , liabilities_new)
    print("Company created successfully!\n")


def none_float_division(f: float | None, g: float | None):
    if f is None or g is None:
        return 'None'
    return '%.2f' % (f / g)


def calculate_formulas(ticker):
    query = session.query(Companies)
    name = query.filter(Companies.ticker == ticker)
    dic_calculos = {}

    for row in name:
        print(f'{row.ticker} {row.name}')

    query_ticker = session.query(Financial)
    for row in query_ticker.filter(Financial.ticker == ticker):
        dic_calculos = {'P/E': none_float_division(row.market_price, row.net_profit),
                        'P/S': none_float_division(row.market_price , row.sales),
                        'P/B': none_float_division(row.market_price, row.assets),
                        'ND/EBITDA':none_float_division(row.net_debt, row.ebitda),
                        'ROE': none_float_division(row.net_profit, row.equity),
                        'ROA': none_float_division(row.net_profit, row.assets),
                        'L/A': none_float_division(row.liabilities, row.assets)
                         }
    return dic_calculos



def print_calculos(dic_calculos):
    for name_value, calculo in dic_calculos.items():
        print(f'{name_value} = {calculo}')


def top_ten(filter):

    if filter == 1:
        print('TICKER ND/EBITDA')
        query = session.query(Financial.ticker, (Financial.net_debt/Financial.ebitda)).order_by(desc(Financial.net_debt/Financial.ebitda))
    elif filter == 2:
        print('TICKER ROE')
        query = session.query(Financial.ticker, (Financial.net_profit/Financial.equity)).order_by(desc(Financial.net_profit/Financial.equity))
    elif filter == 3:
        print('TICKER ROA')
        query = session.query(Financial.ticker, (Financial.net_profit/Financial.assets)).order_by(desc(Financial.net_profit/Financial.assets))

    for row, value in query.limit(10):
        print(f'{row} {round(value, 2)}')
    print('')


def read_company():
    company_enter = input('Enter company name:\n')
    search = "%{}%".format(company_enter)

    query = session.query(Companies)
    c = 0
    lista = []
    for row in query.filter(Companies.name.like(search)):
        print(f'{c} {row.name}')
        c += 1
        lista.append(row.ticker)
    if c == 0:
        print('Company not found!\n')
        return 0
    else:
        number = int(input('Enter company number:\n'))
        return lista[number]


def update_values(ticker):
    ebitda_new, sales_new, profit_new, price_new, net_new, assets_new, equity_new, cash_new, liabilities_new = enter_financial()
    query = session.query(Financial).filter(Financial.ticker == ticker)

    query.update({"ebitda": ebitda_new,
                  "sales": sales_new,
                  "net_profit": profit_new,
                  "market_price": price_new,
                  "net_debt": net_new,
                  "assets": assets_new,
                  "equity": equity_new,
                  "cash_equivalents": cash_new,
                  "liabilities": liabilities_new})
    session.commit()
    print('Company updated successfully!\n')


def delete_values(ticker):
    query_comp = session.query(Companies)
    query_comp.filter(Companies.ticker == ticker).delete()
    query_fin = session.query(Financial)
    query_fin.filter(Financial.ticker == ticker).delete()
    print('Company deleted successfully!\n')
    session.commit()


def list_companies():
    print('COMPANY LIST')
    query = session.query(Companies).order_by(Companies.ticker)
    for row in query:
        print(f'{row.ticker} {row.name} {row.sector}')
    print()


class CalculatorInvestors:
    def __init__(self):
        pass

    def main(self):
        print('Welcome to the Investor Program!\n')
        while True:
            print('MAIN MENU\n0 Exit\n1 CRUD operations\n2 Show top ten companies by criteria\n')
            action = input('Enter an option:\n')
            if action == '0':
                print('Have a nice day!')
                session.close()
                exit()
            else:
                self.handle_action(action)

    def handle_action(self, action):
        if action == '1':
            self.crud_menu()
        elif action == '2':
            self.top_ten_menu()
        else:
            print('Invalid option!\n')

    def handle_action_crud(self, crud_option):
        if crud_option == '1':
            insert_company_new()
        elif crud_option == '2':
            ticker = read_company()
            if ticker != 0:
                dic_calculos = calculate_formulas(ticker)
                print_calculos(dic_calculos)
        elif crud_option == '3':
            ticker = read_company()
            if ticker != 0:
                update_values(ticker)
        elif crud_option == '4':
            ticker = read_company()
            if ticker != 0:
                delete_values(ticker)
        elif crud_option == '5':
            list_companies()
        else:
            print('Invalid option!\n')

    def crud_menu(self):
        print('\nCRUD MENU\n0 Back\n1 Create a company\n2 Read a company\n3 Update a company\n4 Delete a company\n5 List all companies\n')
        crud_option = input('Enter an option:\n')
        self.handle_action_crud(crud_option)

    def top_ten_menu(self):
        print('\nTOP TEN MENU\n0 Back\n1 List by ND/EBITDA\n2 List by ROE\n3 List by ROA\n')
        top_ten_action = input('Enter an option:\n')
        self.handle_action_top_ten(top_ten_action)

    def handle_action_top_ten(self, top_ten_action):
        if top_ten_action == '1':
            top_ten(1)
        elif top_ten_action == '2':
            top_ten(2)
        elif top_ten_action == '3':
            top_ten(3)
        else:
            print('Invalid option!\n')


if __name__ == '__main__':
    #Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    objs = session.query(Companies).all()
    if not len(objs):
        #insertar los datos del csv
        read_data_companies()
        read_data_financial()
        session.commit()
    #menu
    CalculatorInvestors().main()
