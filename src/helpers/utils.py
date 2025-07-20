# Converts into a list of dictionaries, skips empty rows
def rows_to_dicts(data_rows, header):
    return [
        dict(zip(header, row)) for row in data_rows if any(cell.strip() for cell in row)
    ]