using System; 
using System.Collections.Generic; 
using System.Linq; 
using System.Text; 
using System.Threading.Tasks; 
using System.Data; 
using System.Data.SqlClient; 
 
namespace BULK_md_part
{
    class Program
    {
        static void Main(string[] args)
        {
            System.Diagnostics.Stopwatch elapsed = new System.Diagnostics.Stopwatch();
            elapsed.Start(); Int64 rows = 0;
            int error = 0;
            List<String> errors = new List<string>();
            string PathToFile = "C:\\Programming_Projects\\SPA_new\\SPA_SQL_tbls_ww_11_2017\\";
            string fileUploaded = "INV_Item.txt"; //"Part_02.txt";
            string TableName = "INV_Item_ETL"; //"MD_Part_ETL";
            string currentDate = DateTime.Now.ToString("dd_MM_yyyy");
            string Path = @"C:\Programming_Projects\SPA_new\errors\" + currentDate + "_" + TableName + "_errors.txt";
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(@"server=SMNOTN618q.dsmain.ds.corp\INSTPCH1,51433;database=SPA_SQL;trusted_connection=yes",
                System.Data.SqlClient.SqlBulkCopyOptions.FireTriggers|System.Data.SqlClient.SqlBulkCopyOptions.TableLock)
            {
                DestinationTableName = TableName,
                BulkCopyTimeout = 0,
                BatchSize = 200000
            })
            {
            
                using (System.IO.StreamReader reader = new System.IO.StreamReader(PathToFile+fileUploaded))
                {
                    using (DataTable datatable = new DataTable())
                    {
                        //Definition of the column name of the file
                        var columns = datatable.Columns;
                        //Columns of INV_Item_ETL
                        /*
                        columns.Add("BSEG_SYSID", typeof(System.String));
                        columns.Add("BSEG_MANDT", typeof(System.String));
                        columns.Add("BSEG_GJAHR", typeof(System.String));
                        columns.Add("BSEG_BELNR", typeof(System.String));
                        columns.Add("BSEG_BUZEI", typeof(System.String));
                        columns.Add("BSEG_BUZID", typeof(System.String));
                        columns.Add("BSEG_BUKRS", typeof(System.String));
                        columns.Add("BSEG_MENGE", typeof(System.String));
                        columns.Add("BSEG_MEINS", typeof(System.String));
                        columns.Add("BSEG_WRBTR", typeof(System.String));
                        columns.Add("BSEG_DMBTR", typeof(System.String));
                        columns.Add("BSEG_AMOUNTEUR", typeof(System.String));
                        columns.Add("BSEG_LONGTEXT", typeof(System.String));
                        columns.Add("BSEG_SGTXT", typeof(System.String));
                        columns.Add("BSEG_MATNR", typeof(System.String));
                        columns.Add("BSEG_LIFNR", typeof(System.String));
                        columns.Add("BSEG_HKONT", typeof(System.String));
                        columns.Add("BSEG_SAKNR", typeof(System.String));
                        columns.Add("BSEG_WERKS", typeof(System.String));
                        columns.Add("BSEG_KOSTL", typeof(System.String));
                        columns.Add("BSEG_VERTN", typeof(System.String));
                        columns.Add("BSEG_EBELN", typeof(System.String));
                        columns.Add("BSEG_EBELP", typeof(System.String));
                        columns.Add("BSEG_ZEKKN", typeof(System.String));
                        columns.Add("BSEG_AUGDT", typeof(System.String));
                        columns.Add("BSEG_ZUONR", typeof(System.String));
                        columns.Add("BSEG_ZTERM", typeof(System.String));
                        columns.Add("BSEG_BSCHL", typeof(System.String));
                        columns.Add("BSEG_KOART", typeof(System.String));
                        columns.Add("BSEG_SHKZG", typeof(System.String));
                        columns.Add("BSEG_MWSKZ", typeof(System.String));
                        columns.Add("BSEG_MWART", typeof(System.String));
                        columns.Add("BSEG_KTOSL", typeof(System.String));
                        columns.Add("BSEG_BEWAR", typeof(System.String));
                        columns.Add("BSEG_XUMSW", typeof(System.String));
                        columns.Add("BSEG_POSID", typeof(System.String));
                        columns.Add("BSEG_NPLNR", typeof(System.String));
                        columns.Add("BSEG_ZZBKZ", typeof(System.String));
                        columns.Add("BSEG_PRCTR", typeof(System.String));
                        columns.Add("BSEG_AUFNR", typeof(System.String));
                        columns.Add("BSEG_VBEL2", typeof(System.String));
                        columns.Add("BSEG_POSN2", typeof(System.String));
                        columns.Add("BSEG_AUGGJ", typeof(System.String));
                        columns.Add("BSEG_AUGBL", typeof(System.String));
                        columns.Add("BSEG_AGZEI", typeof(System.String));
                        columns.Add("BSEG_ERUSERNAME", typeof(System.String));
                        columns.Add("BSEG_UDATE", typeof(System.String));
                        columns.Add("BSEG_AEUSERNAME", typeof(System.String));
                        columns.Add("BSEG_UDATE_2", typeof(System.String));
                        columns.Add("BSEG_DATUM", typeof(System.String));
                        columns.Add("BSEG_BUDAT", typeof(System.String));
                        columns.Add("BSEG_PROJK", typeof(System.String));
                        columns.Add("BSEG_ERDATE", typeof(System.String));
                        */

                        
                        //MD_Part
                        columns.Add("MARA_SYSID", typeof(System.String));
                        columns.Add("MARA_MANDT", typeof(System.String));
                        columns.Add("MARA_WERKS", typeof(System.String));
                        columns.Add("MARA_MATNR", typeof(System.String));
                        columns.Add("MARA_SPRAS", typeof(System.String));
                        columns.Add("MARA_MAKTX", typeof(System.String));
                        columns.Add("MARA_ZZKT2", typeof(System.String));
                        columns.Add("MARA_MEINS", typeof(System.String));
                        columns.Add("MARA_EKGRP", typeof(System.String));
                        columns.Add("MARA_MATKL", typeof(System.String));
                        columns.Add("MARA_MSTAE", typeof(System.String));
                        columns.Add("MARA_MTART", typeof(System.String));
                        columns.Add("MARA_MTBEZ", typeof(System.String));
                        columns.Add("MARA_ATTYP", typeof(System.String));
                        columns.Add("MARA_DDTEXT", typeof(System.String));
                        columns.Add("MARA_NORMT", typeof(System.String));
                        columns.Add("MARA_PROFL", typeof(System.String));
                        columns.Add("MARA_STOFF", typeof(System.String));
                        columns.Add("MARA_KZUMW", typeof(System.String));
                        columns.Add("MARA_ITARK", typeof(System.String));
                        columns.Add("MARA_MFRNR", typeof(System.String));
                        columns.Add("MARA_MFRPN", typeof(System.String));
                        columns.Add("MARA_MPROF", typeof(System.String));
                        columns.Add("MARA_STPRS", typeof(System.String));
                        columns.Add("MARA_WAERS", typeof(System.String));
                        columns.Add("MARA_PLIFZ", typeof(System.String));
                        columns.Add("MARA_PRDHA", typeof(System.String));
                        columns.Add("MARA_PRCTR", typeof(System.String));
                        columns.Add("MARA_ERSDA", typeof(System.String));
                        columns.Add("MARA_LAEDA", typeof(System.String));
                        columns.Add("MARA_DATUM", typeof(System.String));
                        int batchsize = 0;

                        //Read the first line to skip it and then enter in the loop
                        string headerLine = reader.ReadLine();
                        while (!reader.EndOfStream)
                        {
                            try
                            {
                                string[] line = reader.ReadLine().Split('|');
                                datatable.Rows.Add(line);
                                batchsize += 1;
                                if (batchsize == 100000)
                                {

                                    bulkcopy.WriteToServer(datatable);
                                    datatable.Rows.Clear();
                                    batchsize = 0;
                                    Console.WriteLine("Flushing 100,000 rows");


                                }
                                rows = rows +1;
                            }
                            catch (ArgumentException ex){
                                //Console.Error.Write("Input: \r\n");
                                Console.Error.WriteLine("Input error: the exception message: {0}", ex.Message);
                                //string[] line = reader.ReadLine().Split('|');
                                //addded the problematic line error
                                rows = rows + 1;
                                errors.Add("Error found in line: "+(rows).ToString());
                                //Counter of errors
                                error = error + 1;
                            }

                        }
                        bulkcopy.WriteToServer(datatable);
                        datatable.Rows.Clear();
                    }
                }
            }
            elapsed.Stop();
            
            using (System.IO.TextWriter tw = new System.IO.StreamWriter(Path))
            {
                foreach (string line in errors)
                {
                    tw.WriteLine(string.Format("Line Error: n."+line.ToString()));
                }
            }
            Console.WriteLine((rows-error + " records imported in " + elapsed.Elapsed.TotalSeconds + " seconds. Number of lines skipped because of error: " + error));
            Console.WriteLine("Error lines are linked to the following table: "+TableName);
            Console.WriteLine("Error lines collection are saved in file: \r\n"+Path);
            //saving the list of error s in file under Path
            Console.WriteLine("Upload completed. Push 2 times RETURN to exit...");
            Console.ReadLine();
            
        }
    }
}