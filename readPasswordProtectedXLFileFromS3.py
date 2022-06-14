from pyspark.sql import DataFrame
import io
import msoffcrypto as mso
import pandas as pd
import pyspark.pandas as ps
import s3fs
import openpyxl


def readPasswordProtectedXLFileFromS3(objectPath, password, **kwargs):
    """ A function to read/load a password-protected Excel file without having to manually type it in Excel interface
      Args:
          objectPath (str): The path to the object in the S3 bucket.
          password (str): The password to the object.
          **kwargs: Any additional arguments to pass to the Pandas's read_excel method. E.g. header=1, sheet_name='testSheet'
      Returns:
          DataFrame: Pandas and Spark DataFrames.
    """

    # mount s3 like local fs
    s3 = s3fs.S3FileSystem(anon=False)

    # create an in-memory ByteIO object
    decrypted_wb = io.BytesIO()

    with s3.open(objectPath, 'rb') as xlsfile:

        # open the protected file
        office_file = mso.OfficeFile(xlsfile)

        # provide the password
        office_file.load_key(password=password)

        # decrypt and write to output file
        office_file.decrypt(decrypted_wb)

    # read the output file with pandas
    pandasDf = pd.read_excel(decrypted_wb, engine="openpyxl", **kwargs)

    # extract spark dataframe from pandas dataframe
    sparkDf = ps.from_pandas(pandasDf).to_spark()

    return (pandasDf, sparkDf)
