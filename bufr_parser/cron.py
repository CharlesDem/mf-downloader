from bufr_utils import extract_data_from_bufr


FILE = "test/T_PAGB55_C_EODC_20260126063500.bufr.gz"


def main():
    extract_data_from_bufr(FILE)


if __name__ == "__main__":
    main()