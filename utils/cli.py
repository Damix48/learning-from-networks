import argparse
import populate_db
import create_dataset
import re
import datetime

# Initialize parser
parser = argparse.ArgumentParser()

# Adding subparsers
subparsers = parser.add_subparsers(dest="command")

# Adding db parser
db_parser = subparsers.add_parser('db')
db_parser.add_argument("-y", "--years", help="Enter year(s)")
db_parser.add_argument("-r", "--reset", action='store_true',
                       default=False, help="Reset DB")

# Adding dataset parser
dataset_parser = subparsers.add_parser('dataset')
dataset_parser.add_argument("-y", "--years", help="Enter year(s)")
dataset_parser.add_argument("-p", "--path", help="Enter dataset(s) path")
dataset_parser.add_argument("-n", "--name", help="Enter dataset(s) name")

# Read arguments from command line
args = parser.parse_args()


def parse_year(years: str):
    dates = []
    years = years.replace(' ', '')
    match = re.search(r'(\d+)\*(\d+)', years)
    if match:
        start = int(match.group(1))
        end = int(match.group(2))
        years = years.replace(match.group(0), ','.join(
            map(str, range(start, end+1))))

    for part in years.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            start_date = f"{start}-01-01"
            end_date = f"{end}-12-31"
        elif part == "*":
            start_date = datetime.date.min
            end_date = datetime.date.max
            part = "all"
        else:
            start_date = f"{part}-01-01"
            end_date = f"{part}-12-31"

        dates.append((start_date, end_date, part))

    return dates


if __name__ == '__main__':
    if args.command == 'db':
        if args.years:
            populate_db.populate_db(parse_year(args.years), args.reset)
        else:
            print("Please enter years")
    elif args.command == 'dataset':
        path = args.path if args.path is not None else "./"
        name = args.name if args.name is not None else "dataset"
        years = parse_year(args.years) if args.years is not None else [
            (datetime.date.min, datetime.date.max, "all")]

        create_dataset.create_dataset(path, name, years)
