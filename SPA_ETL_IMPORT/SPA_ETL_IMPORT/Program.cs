using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace SPA_ETL_IMPORT
{
    class Program
    {
        //private static readonly string TableName;

        static void Main(string[] args)
        {


            System.Diagnostics.Stopwatch elapsed = new System.Diagnostics.Stopwatch();
            elapsed.Start(); Int64 rows = 0;
            int error = 0;
            //Month exctration appended...
            string currentMonth = "01_2018";
            List<String> errors = new List<string>();
            string PathToFile = @"C:\Programming_Projects\SPA_new\SPA_SQL_tbls_ww_" + currentMonth + @"\";
            //used for test/// 
            //string PathToFile = @"C:\Programming_Projects\SPA_new\test\";
            //string[] patternFiles = { "QN_Measure.txt", "SO_Item.txt", "Supplier_LFA1.txt", "Savings.txt","PO_History.txt" };
            //Loader per GM_Item esp, da caricare a parte
            //string[] patternFiles = {"GM_Item_ESP.txt"};
            string[] patternFiles = {
                                "Account.txt",
                                "INV_Item.txt",
                                "GM_Item.txt",
                                "CFT_Header.txt",
                                "CFT_Item.txt",
                                "CO_Header.txt",
                                "CO_Item.txt",
                                "CostCenter.txt",
                                "CurrencyMap.txt",
                                "GM_Header.txt", 
                                "INV_Header.txt",
                                "Commodity.txt",
                                "Network.txt",
                                "Part.txt",
                                "PaymentTerm.txt",
                                "Plant.txt",
                                "ProfitCenter.txt",
                                "Project.txt",
                                "PurchasingGroup.txt",
                                "Supplier_LFA1.txt",
                                "Supplier_LFB1.txt",
                                "Supplier_LFM1.txt",
                                "UOM.txt",
                                "User.txt",
                                "PO_Accounting.txt",
                                "PO_Confirmation.txt",
                                "PO_Header.txt",
                                "PO_History.txt",
                                "PO_InfRecGe.txt",
                                "PO_InfRecPOOrg.txt",
                                "PO_Item.txt",
                                "PO_Schedule.txt",
                                "PR.txt",
                                "PR_Accounting.txt",
                                "ProdOrder_Header.txt",
                                "ProdOrder_Item.txt",           
                                "QInfRec.txt",
                                "QN_Action.txt",
                                "QN_Header.txt",
                                "QN_Item.txt",
                                "QN_Measure.txt",
                                "Savings.txt",
                                "SO_Header.txt",
                                "SO_Item.txt"
                                 };

            foreach (string pattern in patternFiles)
                {
                    string TableName = "";
                            if
                            (
                                (pattern.Replace(".txt", "") == "Account")
                                ||
                                (pattern.Replace(".txt", "") == "Commodity")
                                ||
                                (pattern.Replace(".txt", "") == "CostCenter")
                                ||
                                (pattern.Replace(".txt", "") == "CurrencyMap")
                                ||
                                (pattern.Replace(".txt", "") == "Network")
                                ||
                                (pattern.Replace(".txt", "") == "Part")
                                ||
                                (pattern.Replace(".txt", "") == "PaymentTerm")
                                ||
                                (pattern.Replace(".txt", "") == "Plant")
                                 ||
                                (pattern.Replace(".txt", "") == "Project")
                                ||
                                (pattern.Replace(".txt", "") == "ProfitCenter")
                                ||
                                (pattern.Replace(".txt", "") == "PurchasingGroup")
                                ||
                                (pattern.Replace(".txt", "") == "Supplier_LFA1")
                                ||
                                (pattern.Replace(".txt", "") == "Supplier_LFB1")
                                ||
                                (pattern.Replace(".txt", "") == "Supplier_LFM1")
                                ||
                                (pattern.Replace(".txt", "") == "UOM")
                                ||
                                (pattern.Replace(".txt", "") == "User")
                              )
                        {

                            TableName = "MD_" + pattern.Replace(".txt", "") + "_ETL";
                        }
                 
                        else
                        {
                            TableName = pattern.Replace(".txt", "") + "_ETL";
                        }




                foreach (string fileUploaded in Directory.EnumerateFiles(PathToFile, "*.txt"))
                {
                    if (fileUploaded.Contains(pattern)) {

                    var sections = fileUploaded.Split('\\');
                    var fileName = sections[sections.Length - 1];
                    int totalFlushedLines = 0;
                    string currentDate = DateTime.Now.ToString("dd_MM_yyyy");
                    Console.WriteLine("############################################################");
                    Console.WriteLine("");
                    Console.WriteLine("UPLOADING: " + fileName);
                    Console.WriteLine("");
                    //Console.ReadLine();
                    Console.WriteLine("INTO TBLS: " + TableName);
                    Console.WriteLine("############################################################");
                    //Console.ReadLine();

                        string Path = PathToFile + @"errors\" + currentDate + "_" + TableName + "_errors.txt";
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(@"server=SMNOTN618q.dsmain.ds.corp\INSTPCH1,51433;database=SPA_SQL;trusted_connection=yes",
                        System.Data.SqlClient.SqlBulkCopyOptions.FireTriggers | System.Data.SqlClient.SqlBulkCopyOptions.TableLock)
                    {
                        DestinationTableName = TableName,
                        BulkCopyTimeout = 0,
                        BatchSize = 200000
                    })



                    {

                        using (System.IO.StreamReader reader = new System.IO.StreamReader(fileUploaded))
                        {
                            using (DataTable datatable = new DataTable())
                            {
                                var columns = datatable.Columns;
                                    
                                    //-------------------------------MD_Account_ETL Table -------------------------------/
                                    if (fileUploaded.Contains("Account.txt"))
                                    {
                                        columns.Add("SKA1_SYSID", typeof(System.String));
                                        columns.Add("SKA1_MANDT", typeof(System.String));
                                        columns.Add("SKA1_BUKRS", typeof(System.String));
                                        columns.Add("SKA1_SAKNR", typeof(System.String));
                                        columns.Add("SKA1_TXT50", typeof(System.String));
                                        columns.Add("SKA1_KTOPL", typeof(System.String));
                                        columns.Add("SKA1_KTPLT", typeof(System.String));
                                        columns.Add("SKA1_SKB1_ERDAT", typeof(System.String));
                                        columns.Add("SKA1_ERDAT", typeof(System.String));
                                        columns.Add("SKA1_DATUM", typeof(System.String));
                                    }
                                    
                            
                                    //-------------------------------MD_Payment_Term_ETL Table -------------------------------/
                                    else if (fileUploaded.Contains("PaymentTerm.txt"))
                                    {
                                        columns.Add("T052_SYSID", typeof(System.String));
                                        columns.Add("T052_MANDT", typeof(System.String));
                                        columns.Add("T052_ZTERM", typeof(System.String));
                                        columns.Add("T052_SPRAS", typeof(System.String));
                                        columns.Add("T052_TEXT1", typeof(System.String));
                                        columns.Add("T052_ZTAG1", typeof(System.String));
                                        columns.Add("T052_ZPRZ1", typeof(System.String));
                                        columns.Add("T052_ZTAG2", typeof(System.String));
                                        columns.Add("T052_ZPRZ2", typeof(System.String));
                                        columns.Add("T052_ZTAG3", typeof(System.String));
                                        columns.Add("T052_DATUM", typeof(System.String));
                                        columns.Add("T052_ZTAGG", typeof(System.String));

                                    }
                                    //------------------------------- CFT_Header_ETL Table -------------------------------/
                                    else if (fileUploaded.Contains("CFT_Header.txt"))
                                    {
                                        columns.Add("EKKOA_SYSID", typeof(System.String));
                                        columns.Add("EKKOA_MANDT", typeof(System.String));
                                        columns.Add("EKKOA_BUKRS", typeof(System.String));
                                        columns.Add("EKKOA_EBELN", typeof(System.String));
                                        columns.Add("EKKOA_LOEKZ", typeof(System.String));
                                        columns.Add("EKKOA_AEDAT", typeof(System.String));
                                        columns.Add("EKKOA_BEDAT", typeof(System.String));
                                        columns.Add("EKKOA_WAERS", typeof(System.String));
                                        columns.Add("EKKOA_WKURS", typeof(System.String));
                                        columns.Add("EKKOA_KUFIX", typeof(System.String));
                                        columns.Add("EKKOA_CWAERS", typeof(System.String));
                                        columns.Add("EKKOA_LONGTEXT", typeof(System.String));
                                        columns.Add("EKKOA_LIFNR", typeof(System.String));
                                        columns.Add("EKKOA_BSTYP", typeof(System.String));
                                        columns.Add("EKKOA_BSART", typeof(System.String));
                                        columns.Add("EKKOA_INCO1", typeof(System.String));
                                        columns.Add("EKKOA_INCO2", typeof(System.String));
                                        columns.Add("EKKOA_ZTERM", typeof(System.String));
                                        columns.Add("EKKOA_EKORG", typeof(System.String));
                                        columns.Add("EKKOA_EKGRP", typeof(System.String));
                                        columns.Add("EKKOA_IHREZ", typeof(System.String));
                                        columns.Add("EKKOA_UNSEZ", typeof(System.String));
                                        columns.Add("EKKOA_KDATB", typeof(System.String));
                                        columns.Add("EKKOA_KDATE", typeof(System.String));
                                        columns.Add("EKKOA_KONNR", typeof(System.String));
                                        columns.Add("EKKOA_ANGNR", typeof(System.String));
                                        columns.Add("EKKOA_IHRAN", typeof(System.String));
                                        columns.Add("EKKOA_SUBMI", typeof(System.String));
                                        columns.Add("EKKOA_STCEG", typeof(System.String));
                                        columns.Add("EKKOA_EKGSM", typeof(System.String));
                                        columns.Add("EKKOA_UEBDT", typeof(System.String));
                                        columns.Add("EKKOA_ERNAM", typeof(System.String));
                                        columns.Add("EKKOA_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKKOA_UDATE", typeof(System.String));
                                        columns.Add("EKKOA_DATUM", typeof(System.String));

                                    }
                                    //-------------------------------CFT_Item_ETL Table -------------------------------/
                                    else if (fileUploaded.Contains("CFT_Item.txt"))
                                    {
                                        columns.Add("EKPOA_SYSID", typeof(System.String));
                                        columns.Add("EKPOA_MANDT", typeof(System.String));
                                        columns.Add("EKPOA_BUKRS", typeof(System.String));
                                        columns.Add("EKPOA_EBELN", typeof(System.String));
                                        columns.Add("EKPOA_EBELP", typeof(System.String));
                                        columns.Add("EKPOA_WERKS", typeof(System.String));
                                        columns.Add("EKPOA_LOEKZ", typeof(System.String));
                                        columns.Add("EKPOA_PSTYP", typeof(System.String));
                                        columns.Add("EKPOA_UDATE", typeof(System.String));
                                        columns.Add("EKPOA_ORDERDATE", typeof(System.String));
                                        columns.Add("EKPOA_MENGE", typeof(System.String));
                                        columns.Add("EKPOA_MEINS", typeof(System.String));
                                        columns.Add("EKPOA_NETWR", typeof(System.String));
                                        columns.Add("EKPOA_CAMOUNT", typeof(System.String));
                                        columns.Add("EKPOA_EURAMOUNT", typeof(System.String));
                                        columns.Add("EKPOA_WKURS", typeof(System.String));
                                        columns.Add("EKPOA_RATE", typeof(System.String));
                                        columns.Add("EKPOA_LONGTEXT", typeof(System.String));
                                        columns.Add("EKPOA_TXZ01", typeof(System.String));
                                        columns.Add("EKPOA_MATKL", typeof(System.String));
                                        columns.Add("EKPOA_MATNR", typeof(System.String));
                                        columns.Add("EKPOA_REVLV", typeof(System.String));
                                        columns.Add("EKPOA_IDNLF", typeof(System.String));
                                        columns.Add("EKPOA_BANFN", typeof(System.String));
                                        columns.Add("EKPOA_BNFPO", typeof(System.String));
                                        columns.Add("EKPOA_AFNAM", typeof(System.String));
                                        columns.Add("EKPOA_BEDNR", typeof(System.String));
                                        columns.Add("EKPOA_KONNR", typeof(System.String));
                                        columns.Add("EKPOA_KTPNR", typeof(System.String));
                                        columns.Add("EKPOA_WEPOS", typeof(System.String));
                                        columns.Add("EKPOA_EVERS", typeof(System.String));
                                        columns.Add("EKPOA_INSMK", typeof(System.String));
                                        columns.Add("EKPOA_MHDRZ", typeof(System.String));
                                        columns.Add("EKPOA_IPRKZ", typeof(System.String));
                                        columns.Add("EKPOA_SSQSS", typeof(System.String));
                                        columns.Add("EKPOA_PLIFZ", typeof(System.String));
                                        columns.Add("EKPOA_WEBAZ", typeof(System.String));
                                        columns.Add("EKPOA_LEWED", typeof(System.String));
                                        columns.Add("EKPOA_ABLAD", typeof(System.String));
                                        columns.Add("EKPOA_LGORT", typeof(System.String));
                                        columns.Add("EKPOA_NAME1", typeof(System.String));
                                        columns.Add("EKPOA_NAME2", typeof(System.String));
                                        columns.Add("EKPOA_STREET", typeof(System.String));
                                        columns.Add("EKPOA_POST_CODE1", typeof(System.String));
                                        columns.Add("EKPOA_CITY1", typeof(System.String));
                                        columns.Add("EKPOA_REGION", typeof(System.String));
                                        columns.Add("EKPOA_COUNTRY", typeof(System.String));
                                        columns.Add("EKPOA_PO_BOX", typeof(System.String));
                                        columns.Add("EKPOA_KZABS", typeof(System.String));
                                        columns.Add("EKPOA_LABNR", typeof(System.String));
                                        columns.Add("EKPOA_ELIKZ", typeof(System.String));
                                        columns.Add("EKPOA_EREKZ", typeof(System.String));
                                        columns.Add("EKPOA_UEBTO", typeof(System.String));
                                        columns.Add("EKPOA_UEBTK", typeof(System.String));
                                        columns.Add("EKPOA_UNTTO", typeof(System.String));
                                        columns.Add("EKPOA_XOBLR", typeof(System.String));
                                        columns.Add("EKPOA_KNTTP", typeof(System.String));
                                        columns.Add("EKPOA_VRTKZ", typeof(System.String));
                                        columns.Add("EKPOA_INCO1", typeof(System.String));
                                        columns.Add("EKPOA_INCO2", typeof(System.String));
                                        columns.Add("EKPOA_MFRNR", typeof(System.String));
                                        columns.Add("EKPOA_MFRPN", typeof(System.String));
                                        columns.Add("EKPOA_EAN11", typeof(System.String));
                                        columns.Add("EKPOA_ERUSERNAME", typeof(System.String));
                                        columns.Add("EKPOA_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKPOA_AEDAT", typeof(System.String));
                                        columns.Add("EKPOA_DATUM", typeof(System.String));
                                        columns.Add("EKPOA_ZZBP1", typeof(System.String));
                                        columns.Add("EKPOA_ZZCR1", typeof(System.String));
                                        columns.Add("EKPOA_ZZPT1", typeof(System.String));
                                        columns.Add("EKPOA_ZZPD1", typeof(System.String));
                                        columns.Add("EKPOA_ZZBP2", typeof(System.String));
                                        columns.Add("EKPOA_ZZCR2", typeof(System.String));
                                        columns.Add("EKPOA_ZZPT2", typeof(System.String));
                                        columns.Add("EKPOA_ZZPD2", typeof(System.String));
                                        columns.Add("EKPOA_ZZBP3", typeof(System.String));
                                        columns.Add("EKPOA_ZZCR3", typeof(System.String));
                                        columns.Add("EKPOA_ZZPT3", typeof(System.String));
                                        columns.Add("EKPOA_ZZPD3", typeof(System.String));
                                        columns.Add("EKPOA_ZZBP4", typeof(System.String));
                                        columns.Add("EKPOA_ZZCR4", typeof(System.String));
                                        columns.Add("EKPOA_ZZPT4", typeof(System.String));
                                        columns.Add("EKPOA_ZZPD4", typeof(System.String));
                                        columns.Add("EKPOA_ZZBP5", typeof(System.String));
                                        columns.Add("EKPOA_ZZCR5", typeof(System.String));
                                        columns.Add("EKPOA_ZZPT5", typeof(System.String));
                                        columns.Add("EKPOA_ZZPD5", typeof(System.String));

                                    }
                                    //------------------------------- CO_Header_ETL Table -------------------------------/
                                    else if (fileUploaded.Contains("CO_Header.txt"))
                                    {
                                        columns.Add("EKKOK_SYSID", typeof(System.String));
                                        columns.Add("EKKOK_MANDT", typeof(System.String));
                                        columns.Add("EKKOK_BUKRS", typeof(System.String));
                                        columns.Add("EKKOK_EBELN", typeof(System.String));
                                        columns.Add("EKKOK_LOEKZ", typeof(System.String));
                                        columns.Add("EKKOK_AEDAT", typeof(System.String));
                                        columns.Add("EKKOK_BEDAT", typeof(System.String));
                                        columns.Add("EKKOK_WAERS", typeof(System.String));
                                        columns.Add("EKKOK_WKURS", typeof(System.String));
                                        columns.Add("EKKOK_KUFIX", typeof(System.String));
                                        columns.Add("EKKOK_CWAERS", typeof(System.String));
                                        columns.Add("EKKOK_LONGTEXT", typeof(System.String));
                                        columns.Add("EKKOK_LIFNR", typeof(System.String));
                                        columns.Add("EKKOK_BSTYP", typeof(System.String));
                                        columns.Add("EKKOK_BSART", typeof(System.String));
                                        columns.Add("EKKOK_INCO1", typeof(System.String));
                                        columns.Add("EKKOK_INCO2", typeof(System.String));
                                        columns.Add("EKKOK_ZTERM", typeof(System.String));
                                        columns.Add("EKKOK_EKORG", typeof(System.String));
                                        columns.Add("EKKOK_EKGRP", typeof(System.String));
                                        columns.Add("EKKOK_IHREZ", typeof(System.String));
                                        columns.Add("EKKOK_UNSEZ", typeof(System.String));
                                        columns.Add("EKKOK_KDATB", typeof(System.String));
                                        columns.Add("EKKOK_KDATE", typeof(System.String));
                                        columns.Add("EKKOK_KONNR", typeof(System.String));
                                        columns.Add("EKKOK_ANGNR", typeof(System.String));
                                        columns.Add("EKKOK_IHRAN", typeof(System.String));
                                        columns.Add("EKKOK_SUBMI", typeof(System.String));
                                        columns.Add("EKKOK_STCEG", typeof(System.String));
                                        columns.Add("EKKOK_EKGSM", typeof(System.String));
                                        columns.Add("EKKOK_UEBDT", typeof(System.String));
                                        columns.Add("EKKOK_ERNAM", typeof(System.String));
                                        columns.Add("EKKOK_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKKOK_UDATE", typeof(System.String));
                                        columns.Add("EKKOK_DATUM", typeof(System.String));

                                    }
                                    //------------------------------- CO_Item_ETL Table -------------------------------/
                                    else if (fileUploaded.Contains("CO_Item.txt"))
                                    {
                                        columns.Add("EKPOK_SYSID", typeof(System.String));
                                        columns.Add("EKPOK_MANDT", typeof(System.String));
                                        columns.Add("EKPOK_BUKRS", typeof(System.String));
                                        columns.Add("EKPOK_EBELN", typeof(System.String));
                                        columns.Add("EKPOK_EBELP", typeof(System.String));
                                        columns.Add("EKPOK_WERKS", typeof(System.String));
                                        columns.Add("EKPOK_LOEKZ", typeof(System.String));
                                        columns.Add("EKPOK_PSTYP", typeof(System.String));
                                        columns.Add("EKPOK_UDATE", typeof(System.String));
                                        columns.Add("EKPOK_ORDERDATE", typeof(System.String));
                                        columns.Add("EKPOK_MENGE", typeof(System.String));
                                        columns.Add("EKPOK_MEINS", typeof(System.String));
                                        columns.Add("EKPOK_NETWR", typeof(System.String));
                                        columns.Add("EKPOK_CAMOUNT", typeof(System.String));
                                        columns.Add("EKPOK_EURAMOUNT", typeof(System.String));
                                        columns.Add("EKPOK_WKURS", typeof(System.String));
                                        columns.Add("EKPOK_RATE", typeof(System.String));
                                        columns.Add("EKPOK_LONGTEXT", typeof(System.String));
                                        columns.Add("EKPOK_TXZ01", typeof(System.String));
                                        columns.Add("EKPOK_MATKL", typeof(System.String));
                                        columns.Add("EKPOK_MATNR", typeof(System.String));
                                        columns.Add("EKPOK_REVLV", typeof(System.String));
                                        columns.Add("EKPOK_IDNLF", typeof(System.String));
                                        columns.Add("EKPOK_BANFN", typeof(System.String));
                                        columns.Add("EKPOK_BNFPO", typeof(System.String));
                                        columns.Add("EKPOK_AFNAM", typeof(System.String));
                                        columns.Add("EKPOK_BEDNR", typeof(System.String));
                                        columns.Add("EKPOK_KONNR", typeof(System.String));
                                        columns.Add("EKPOK_KTPNR", typeof(System.String));
                                        columns.Add("EKPOK_WEPOS", typeof(System.String));
                                        columns.Add("EKPOK_EVERS", typeof(System.String));
                                        columns.Add("EKPOK_INSMK", typeof(System.String));
                                        columns.Add("EKPOK_MHDRZ", typeof(System.String));
                                        columns.Add("EKPOK_IPRKZ", typeof(System.String));
                                        columns.Add("EKPOK_SSQSS", typeof(System.String));
                                        columns.Add("EKPOK_PLIFZ", typeof(System.String));
                                        columns.Add("EKPOK_WEBAZ", typeof(System.String));
                                        columns.Add("EKPOK_LEWED", typeof(System.String));
                                        columns.Add("EKPOK_ABLAD", typeof(System.String));
                                        columns.Add("EKPOK_LGORT", typeof(System.String));
                                        columns.Add("EKPOK_NAME1", typeof(System.String));
                                        columns.Add("EKPOK_NAME2", typeof(System.String));
                                        columns.Add("EKPOK_STREET", typeof(System.String));
                                        columns.Add("EKPOK_POST_CODE1", typeof(System.String));
                                        columns.Add("EKPOK_CITY1", typeof(System.String));
                                        columns.Add("EKPOK_REGION", typeof(System.String));
                                        columns.Add("EKPOK_COUNTRY", typeof(System.String));
                                        columns.Add("EKPOK_PO_BOX", typeof(System.String));
                                        columns.Add("EKPOK_KZABS", typeof(System.String));
                                        columns.Add("EKPOK_LABNR", typeof(System.String));
                                        columns.Add("EKPOK_ELIKZ", typeof(System.String));
                                        columns.Add("EKPOK_EREKZ", typeof(System.String));
                                        columns.Add("EKPOK_UEBTO", typeof(System.String));
                                        columns.Add("EKPOK_UEBTK", typeof(System.String));
                                        columns.Add("EKPOK_UNTTO", typeof(System.String));
                                        columns.Add("EKPOK_XOBLR", typeof(System.String));
                                        columns.Add("EKPOK_KNTTP", typeof(System.String));
                                        columns.Add("EKPOK_VRTKZ", typeof(System.String));
                                        columns.Add("EKPOK_INCO1", typeof(System.String));
                                        columns.Add("EKPOK_INCO2", typeof(System.String));
                                        columns.Add("EKPOK_MFRNR", typeof(System.String));
                                        columns.Add("EKPOK_MFRPN", typeof(System.String));
                                        columns.Add("EKPOK_EAN11", typeof(System.String));
                                        columns.Add("EKPOK_ERUSERNAME", typeof(System.String));
                                        columns.Add("EKPOK_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKPOK_AEDAT", typeof(System.String));
                                        columns.Add("EKPOK_DATUM", typeof(System.String));
                                        columns.Add("EKPOK_ZZBP1", typeof(System.String));
                                        columns.Add("EKPOK_ZZCR1", typeof(System.String));
                                        columns.Add("EKPOK_ZZPT1", typeof(System.String));
                                        columns.Add("EKPOK_ZZPD1", typeof(System.String));
                                        columns.Add("EKPOK_ZZBP2", typeof(System.String));
                                        columns.Add("EKPOK_ZZCR2", typeof(System.String));
                                        columns.Add("EKPOK_ZZPT2", typeof(System.String));
                                        columns.Add("EKPOK_ZZPD2", typeof(System.String));
                                        columns.Add("EKPOK_ZZBP3", typeof(System.String));
                                        columns.Add("EKPOK_ZZCR3", typeof(System.String));
                                        columns.Add("EKPOK_ZZPT3", typeof(System.String));
                                        columns.Add("EKPOK_ZZPD3", typeof(System.String));
                                        columns.Add("EKPOK_ZZBP4", typeof(System.String));
                                        columns.Add("EKPOK_ZZCR4", typeof(System.String));
                                        columns.Add("EKPOK_ZZPT4", typeof(System.String));
                                        columns.Add("EKPOK_ZZPD4", typeof(System.String));
                                        columns.Add("EKPOK_ZZBP5", typeof(System.String));
                                        columns.Add("EKPOK_ZZCR5", typeof(System.String));
                                        columns.Add("EKPOK_ZZPT5", typeof(System.String));
                                        columns.Add("EKPOK_ZZPD5", typeof(System.String));

                                    }
                                    //-------------------------------MD_Commodity_ETL Table -------------------------------/
                                    else if (fileUploaded.Contains("Commodity.txt"))
                                    {
                                        columns.Add("T023T_SYSID", typeof(System.String));
                                        columns.Add("T023T_MANDT", typeof(System.String));
                                        columns.Add("T023T_SPRAS", typeof(System.String));
                                        columns.Add("T023T_MATKL", typeof(System.String));
                                        columns.Add("T023T_WGBEZ", typeof(System.String));
                                        columns.Add("T023T_WGBEZ60", typeof(System.String));
                                        columns.Add("T023T_DATUM", typeof(System.String));

                                    }
                                    //-------------------------------MD_CostCenter_ETL Table -------------------------------/
                                    else if (fileUploaded.Contains("CostCenter.txt"))
                                    {
                                        columns.Add("CSKS_SYSID", typeof(System.String));
                                        columns.Add("CSKS_MANDT", typeof(System.String));
                                        columns.Add("CSKS_KOKRS", typeof(System.String));
                                        columns.Add("CSKS_KOSTL", typeof(System.String));
                                        columns.Add("CSKS_SPRAS", typeof(System.String));
                                        columns.Add("CSKS_KTEXT", typeof(System.String));
                                        columns.Add("CSKS_LTEXT", typeof(System.String));
                                        columns.Add("CSKS_VERAK", typeof(System.String));
                                        columns.Add("CSKS_ABTEI", typeof(System.String));
                                        columns.Add("CSKS_RECID", typeof(System.String));
                                        columns.Add("CSKS_KHINR", typeof(System.String));
                                        columns.Add("CSKS_FUNC_AREA", typeof(System.String));
                                        columns.Add("CSKS_PRCTR", typeof(System.String));
                                        columns.Add("CSKS_DATAB", typeof(System.String));
                                        columns.Add("CSKS_DATBI", typeof(System.String));
                                        columns.Add("CSKS_ERSDA", typeof(System.String));
                                        columns.Add("CSKS_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- GM_Header_ETL Table -------------------------------/
                                    else if (fileUploaded.Contains("GM_Header.txt"))
                                    {
                                        columns.Add("MKPF_SYSID", typeof(System.String));
                                        columns.Add("MKPF_MANDT", typeof(System.String));
                                        columns.Add("MKPF_MBLNR", typeof(System.String));
                                        columns.Add("MKPF_MJAHR", typeof(System.String));
                                        columns.Add("MKPF_VGART", typeof(System.String));
                                        columns.Add("MKPF_BLART", typeof(System.String));
                                        columns.Add("MKPF_BLDAT", typeof(System.String));
                                        columns.Add("MKPF_BUDAT", typeof(System.String));
                                        columns.Add("MKPF_CPUDT", typeof(System.String));
                                        columns.Add("MKPF_ZZBARCODE", typeof(System.String));
                                        columns.Add("MKPF_BKTXT", typeof(System.String));
                                        columns.Add("MKPF_XBLNR", typeof(System.String));
                                        columns.Add("MKPF_USNAM", typeof(System.String));
                                        columns.Add("MKPF_AEDAT", typeof(System.String));
                                        columns.Add("MKPF_DATUM", typeof(System.String));
                                    }
                                                                       
                                    //------------------------------- GM_Item_ETL Table ------------------------------ALL/
                                    
                                    else if (fileUploaded.Contains("GM_Item.txt"))
                                    {
                                        columns.Add("MSEG_SYSID", typeof(System.String));
                                        columns.Add("MSEG_MANDT", typeof(System.String));
                                        columns.Add("MSEG_MBLNR", typeof(System.String));
                                        columns.Add("MSEG_ZEILE", typeof(System.String));
                                        columns.Add("MSEG_MJAHR", typeof(System.String));
                                        columns.Add("MSEG_XAUTO", typeof(System.String));
                                        columns.Add("MSEG_BUKRS", typeof(System.String));
                                        columns.Add("MSEG_WERKS", typeof(System.String));
                                        columns.Add("MSEG_BUDAT_MKPF", typeof(System.String));
                                        columns.Add("MSEG_CPUDT_MKPF", typeof(System.String));
                                        columns.Add("MSEG_BWART", typeof(System.String));
                                        columns.Add("MSEG_SHKZG", typeof(System.String));
                                        columns.Add("MSEG_MENGE", typeof(System.String));
                                        columns.Add("MSEG_MEINS", typeof(System.String));
                                        columns.Add("MSEG_ERFMG", typeof(System.String));
                                        columns.Add("MSEG_ERFME", typeof(System.String));
                                        columns.Add("MSEG_BPMNG", typeof(System.String));
                                        columns.Add("MSEG_BPRME", typeof(System.String));
                                        columns.Add("MSEG_BSTMG", typeof(System.String));
                                        columns.Add("MSEG_BSTME", typeof(System.String));
                                        columns.Add("MSEG_EBELN", typeof(System.String));
                                        columns.Add("MSEG_EBELP", typeof(System.String));
                                        columns.Add("MSEG_SGTXT", typeof(System.String));
                                        columns.Add("MSEG_LIFNR", typeof(System.String));
                                        columns.Add("MSEG_SAKTO", typeof(System.String));
                                        columns.Add("MSEG_KOSTL", typeof(System.String));
                                        columns.Add("MSEG_POSID", typeof(System.String));
                                        columns.Add("MSEG_NPLNR", typeof(System.String));
                                        columns.Add("MSEG_PRCTR", typeof(System.String));
                                        columns.Add("MSEG_LGORT", typeof(System.String));
                                        columns.Add("MSEG_UMLGO", typeof(System.String));
                                        columns.Add("MSEG_LGNUM", typeof(System.String));
                                        columns.Add("MSEG_LGTYP", typeof(System.String));
                                        columns.Add("MSEG_LGPLA", typeof(System.String));
                                        columns.Add("MSEG_BWLVS", typeof(System.String));
                                        columns.Add("MSEG_TBNUM", typeof(System.String));
                                        columns.Add("MSEG_WEMPF", typeof(System.String));
                                        columns.Add("MSEG_SJAHR", typeof(System.String));
                                        columns.Add("MSEG_SMBLN", typeof(System.String));
                                        columns.Add("MSEG_SMBLP", typeof(System.String));
                                        columns.Add("MSEG_XWSBR", typeof(System.String));
                                        columns.Add("MSEG_GRUND", typeof(System.String));
                                        columns.Add("MSEG_USNAM_MKPF", typeof(System.String));
                                        columns.Add("MSEG_/BEV2/ED_AEDAT", typeof(System.String));
                                        columns.Add("MSEG_DATUM", typeof(System.String));
                                    }
                                    
   
                                    //------------------------------- INV_Header_ETL Table -------------------------------
                                    else if (fileUploaded.Contains("INV_Header.txt"))
                                    {
                                        columns.Add("BKPF_SYSID", typeof(System.String));
                                        columns.Add("BKPF_MANDT", typeof(System.String));
                                        columns.Add("BKPF_GJAHR", typeof(System.String));
                                        columns.Add("BKPF_BELNR", typeof(System.String));
                                        columns.Add("BKPF_BUKRS", typeof(System.String));
                                        columns.Add("BKPF_BUDAT", typeof(System.String));
                                        columns.Add("BKPF_CPUDT", typeof(System.String));
                                        columns.Add("BKPF_BLDAT", typeof(System.String));
                                        columns.Add("BKPF_WAERS", typeof(System.String));
                                        columns.Add("BKPF_KURSF", typeof(System.String));
                                        columns.Add("BKPF_WWERT", typeof(System.String));
                                        columns.Add("BKPF_HWAER", typeof(System.String));
                                        columns.Add("BKPF_BKTXT", typeof(System.String));
                                        columns.Add("BKPF_BLART", typeof(System.String));
                                        columns.Add("BKPF_XBLNR", typeof(System.String));
                                        columns.Add("BKPF_BSTAT", typeof(System.String));
                                        columns.Add("BKPF_STJAH", typeof(System.String));
                                        columns.Add("BKPF_STBLG", typeof(System.String));
                                        columns.Add("BKPF_XREVERSAL", typeof(System.String));
                                        columns.Add("BKPF_ERUSERNAME", typeof(System.String));
                                        columns.Add("BKPF_AEUSERNAME", typeof(System.String));
                                        columns.Add("BKPF_AEDAT", typeof(System.String));
                                        columns.Add("BKPF_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- INV_Item_ETL Table -------------------------------//
                                    
                                    else if (fileUploaded.Contains("INV_Item.txt"))
                                    {
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
                                        columns.Add("BSEG_ERDATE", typeof(System.String));
                                        columns.Add("BSEG_AEUSERNAME", typeof(System.String));
                                        columns.Add("BSEG_UDATE", typeof(System.String));
                                        columns.Add("BSEG_DATUM", typeof(System.String));
                                    }
                                    
                                    
                                    //-------------------------------MD_Network_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("Network.txt"))
                                    {
                                        columns.Add("AUFK_SYSID", typeof(System.String));
                                        columns.Add("AUFK_MANDT", typeof(System.String));
                                        columns.Add("AUFK_AUFNR", typeof(System.String));
                                        columns.Add("AUFK_KTEXT", typeof(System.String));
                                        columns.Add("AUFK_LTEXT", typeof(System.String));
                                        columns.Add("AUFK_AUART", typeof(System.String));
                                        columns.Add("AUFK_AUTYP", typeof(System.String));
                                        columns.Add("AUFK_BUKRS", typeof(System.String));
                                        columns.Add("AUFK_WERKS", typeof(System.String));
                                        columns.Add("AUFK_WAERS", typeof(System.String));
                                        columns.Add("AUFK_PRCTR", typeof(System.String));
                                        columns.Add("AUFK_FUNC_AREA", typeof(System.String));
                                        columns.Add("AUFK_SCOPE", typeof(System.String));
                                        columns.Add("AUFK_PSPEL", typeof(System.String));
                                        columns.Add("AUFK_KDAUF", typeof(System.String));
                                        columns.Add("AUFK_KDPOS", typeof(System.String));
                                        columns.Add("AUFK_VERS", typeof(System.String));
                                        columns.Add("AUFK_ID_1", typeof(System.String));
                                        columns.Add("AUFK_ID_2", typeof(System.String));
                                        columns.Add("AUFK_ID_3", typeof(System.String));
                                        columns.Add("AUFK_SGE", typeof(System.String));
                                        columns.Add("AUFK_PMO", typeof(System.String));
                                        columns.Add("AUFK_VALID", typeof(System.String));
                                        columns.Add("AUFK_BEA_DAT", typeof(System.String));
                                        columns.Add("AUFK_ERNAM", typeof(System.String));
                                        columns.Add("AUFK_ERDAT", typeof(System.String));
                                        columns.Add("AUFK_AENAM", typeof(System.String));
                                        columns.Add("AUFK_AEDAT", typeof(System.String));
                                        columns.Add("AUFK_DATUM", typeof(System.String));

                                    }
                                    //------------------------------- MD_Part_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("Part.txt"))
                                    {
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
                                    }
                                    //------------------------------- MD_Plant_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("Plant.txt"))
                                    {
                                        columns.Add("T001W_SYSID", typeof(System.String));
                                        columns.Add("T001W_MANDT", typeof(System.String));
                                        columns.Add("T001W_WERKS", typeof(System.String));
                                        columns.Add("T001W_NAME1", typeof(System.String));
                                        columns.Add("T001W_NAME2", typeof(System.String));
                                        columns.Add("T001W_STRAS", typeof(System.String));
                                        columns.Add("T001W_ORT01", typeof(System.String));
                                        columns.Add("T001W_REGIO", typeof(System.String));
                                        columns.Add("T001W_LAND1", typeof(System.String));
                                        columns.Add("T001W_PSTLZ", typeof(System.String));
                                        columns.Add("T001W_BEZEI", typeof(System.String));
                                        columns.Add("T001W_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- PO_Accounting_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PO_Accounting.txt"))
                                    {
                                        columns.Add("EKKN_SYSID", typeof(System.String));
                                        columns.Add("EKKN_MANDT", typeof(System.String));
                                        columns.Add("EKKN_EBELN", typeof(System.String));
                                        columns.Add("EKKN_EBELP", typeof(System.String));
                                        columns.Add("EKKN_LOEKZ", typeof(System.String));
                                        columns.Add("EKKN_AEDAT", typeof(System.String));
                                        columns.Add("EKKN_MENGE", typeof(System.String));
                                        columns.Add("EKKN_VPROZ", typeof(System.String));
                                        columns.Add("EKKN_NETWR", typeof(System.String));
                                        columns.Add("EKKN_ABLAD", typeof(System.String));
                                        columns.Add("EKKN_ZEKKN", typeof(System.String));
                                        columns.Add("EKKN_VETEN", typeof(System.String));
                                        columns.Add("EKKN_SAKTO", typeof(System.String));
                                        columns.Add("EKKN_KOSTL", typeof(System.String));
                                        columns.Add("EKKN_VBELN", typeof(System.String));
                                        columns.Add("EKKN_VBELP", typeof(System.String));
                                        columns.Add("EKKN_AUFNR", typeof(System.String));
                                        columns.Add("EKKN_PRCTR", typeof(System.String));
                                        columns.Add("EKKN_POSID", typeof(System.String));
                                        columns.Add("EKKN_NPLNR", typeof(System.String));
                                        columns.Add("EKKN_ZZBKZ", typeof(System.String));
                                        columns.Add("EKKN_ANLN1", typeof(System.String));
                                        columns.Add("EKKN_ANLN2", typeof(System.String));
                                        columns.Add("EKKN_WEMPF", typeof(System.String));
                                        columns.Add("EKKN_AA_FINAL_IND", typeof(System.String));
                                        columns.Add("EKKN_AA_FINAL_QTY", typeof(System.String));
                                        columns.Add("EKKN_AA_FINAL_REASON", typeof(System.String));
                                        columns.Add("EKKN_EREKZ", typeof(System.String));
                                        columns.Add("EKKN_PS_PSP_PNR", typeof(System.String));
                                        columns.Add("EKKN_ERUSERNAME", typeof(System.String));
                                        columns.Add("EKKN_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKKN_UDATE", typeof(System.String));
                                        columns.Add("EKKN_DATUM", typeof(System.String));
                                    }
                                    //-------------------------------MD_CurrencyMap Table -------------------------------//
                                    else if (fileUploaded.Contains("CurrencyMap.txt"))
                                    {
                                        columns.Add("TCURC_SYSID", typeof(System.String));
                                        columns.Add("TCURC_MANDT", typeof(System.String));
                                        columns.Add("TCURC_WAERS", typeof(System.String));
                                        columns.Add("TCURC_ISOCD", typeof(System.String));
                                        columns.Add("TCURC_SPRAS", typeof(System.String));
                                        columns.Add("TCURC_LTEXT", typeof(System.String));
                                        columns.Add("TCURC_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- PO_Confirmation_ETL Table ------------------------------//
                                    else if (fileUploaded.Contains("PO_Confirmation.txt"))
                                    {
                                        columns.Add("EKES_SYSID", typeof(System.String));
                                        columns.Add("EKES_MANDT", typeof(System.String));
                                        columns.Add("EKES_EBELN", typeof(System.String));
                                        columns.Add("EKES_EBELP", typeof(System.String));
                                        columns.Add("EKES_ETENS", typeof(System.String));
                                        columns.Add("EKES_EBTYP", typeof(System.String));
                                        columns.Add("EKES_EINDT", typeof(System.String));
                                        columns.Add("EKES_LPEIN", typeof(System.String));
                                        columns.Add("EKES_UZEIT", typeof(System.String));
                                        columns.Add("EKES_ERDAT", typeof(System.String));
                                        columns.Add("EKES_EZEIT", typeof(System.String));
                                        columns.Add("EKES_MENGE", typeof(System.String));
                                        columns.Add("EKES_DABMG", typeof(System.String));
                                        columns.Add("EKES_ESTKZ", typeof(System.String));
                                        columns.Add("EKES_LOEKZ", typeof(System.String));
                                        columns.Add("EKES_KZDIS", typeof(System.String));
                                        columns.Add("EKES_XBLNR", typeof(System.String));
                                        columns.Add("EKES_VBELN", typeof(System.String));
                                        columns.Add("EKES_VBELP", typeof(System.String));
                                        columns.Add("EKES_MPROF", typeof(System.String));
                                        columns.Add("EKES_EMATN", typeof(System.String));
                                        columns.Add("EKES_MAHNZ", typeof(System.String));
                                        columns.Add("EKES_CHARG", typeof(System.String));
                                        columns.Add("EKES_UECHA", typeof(System.String));
                                        columns.Add("EKES_REF_ETENS", typeof(System.String));
                                        columns.Add("EKES_IMWRK", typeof(System.String));
                                        columns.Add("EKES_VBELN_ST", typeof(System.String));
                                        columns.Add("EKES_VBELP_ST", typeof(System.String));
                                        columns.Add("EKES_J_3AETENR", typeof(System.String));
                                        columns.Add("EKES_ERUSERNAME", typeof(System.String));
                                        columns.Add("EKES_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKES_UDATE", typeof(System.String));
                                        columns.Add("EKES_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- PO_Header_ETl Table -------------------------------//
                                    else if (fileUploaded.Contains("PO_Header.txt"))
                                    {
                                        columns.Add("EKKO_SYSID", typeof(System.String));
                                        columns.Add("EKKO_MANDT", typeof(System.String));
                                        columns.Add("EKKO_BUKRS", typeof(System.String));
                                        columns.Add("EKKO_EBELN", typeof(System.String));
                                        columns.Add("EKKO_LOEKZ", typeof(System.String));
                                        columns.Add("EKKO_AEDAT", typeof(System.String));
                                        columns.Add("EKKO_BEDAT", typeof(System.String));
                                        columns.Add("EKKO_WAERS", typeof(System.String));
                                        columns.Add("EKKO_WKURS", typeof(System.String));
                                        columns.Add("EKKO_KUFIX", typeof(System.String));
                                        columns.Add("EKKO_CWAERS", typeof(System.String));
                                        columns.Add("EKKO_LONGTEXT", typeof(System.String));
                                        columns.Add("EKKO_LIFNR", typeof(System.String));
                                        columns.Add("EKKO_BSTYP", typeof(System.String));
                                        columns.Add("EKKO_BSART", typeof(System.String));
                                        columns.Add("EKKO_INCO1", typeof(System.String));
                                        columns.Add("EKKO_INCO2", typeof(System.String));
                                        columns.Add("EKKO_ZTERM", typeof(System.String));
                                        columns.Add("EKKO_EKORG", typeof(System.String));
                                        columns.Add("EKKO_EKGRP", typeof(System.String));
                                        columns.Add("EKKO_IHREZ", typeof(System.String));
                                        columns.Add("EKKO_UNSEZ", typeof(System.String));
                                        columns.Add("EKKO_KDATB", typeof(System.String));
                                        columns.Add("EKKO_KDATE", typeof(System.String));
                                        columns.Add("EKKO_KONNR", typeof(System.String));
                                        columns.Add("EKKO_ANGNR", typeof(System.String));
                                        columns.Add("EKKO_IHRAN", typeof(System.String));
                                        columns.Add("EKKO_SUBMI", typeof(System.String));
                                        columns.Add("EKKO_STCEG", typeof(System.String));
                                        columns.Add("EKKO_EKGSM", typeof(System.String));
                                        columns.Add("EKKO_UEBDT", typeof(System.String));
                                        columns.Add("EKKO_ERNAM", typeof(System.String));
                                        columns.Add("EKKO_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKKO_UDATE", typeof(System.String));
                                        columns.Add("EKKO_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- PO_History_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PO_History.txt"))
                                    {
                                        columns.Add("EKBE_SYSID", typeof(System.String));
                                        columns.Add("EKBE_MANDT", typeof(System.String));
                                        columns.Add("EKBE_EBELN", typeof(System.String));
                                        columns.Add("EKBE_EBELP", typeof(System.String));
                                        columns.Add("EKBE_AREWB", typeof(System.String));
                                        columns.Add("EKBE_AREWR", typeof(System.String));
                                        columns.Add("EKBE_AREWR_POP", typeof(System.String));
                                        columns.Add("EKBE_AREWW", typeof(System.String));
                                        columns.Add("EKBE_BAMNG", typeof(System.String));
                                        columns.Add("EKBE_BEKKN", typeof(System.String));
                                        columns.Add("EKBE_BELNR", typeof(System.String));
                                        columns.Add("EKBE_BEWTP", typeof(System.String));
                                        columns.Add("EKBE_BLDAT", typeof(System.String));
                                        columns.Add("EKBE_BPMNG", typeof(System.String));
                                        columns.Add("EKBE_BPMNG_POP", typeof(System.String));
                                        columns.Add("EKBE_BPWEB", typeof(System.String));
                                        columns.Add("EKBE_BPWES", typeof(System.String));
                                        columns.Add("EKBE_BUDAT", typeof(System.String));
                                        columns.Add("EKBE_BUZEI", typeof(System.String));
                                        columns.Add("EKBE_BWART", typeof(System.String));
                                        columns.Add("EKBE_BWTAR", typeof(System.String));
                                        columns.Add("EKBE_CHARG", typeof(System.String));
                                        columns.Add("EKBE_CPUDT", typeof(System.String));
                                        columns.Add("EKBE_CPUTM", typeof(System.String));
                                        columns.Add("EKBE_DMBTR", typeof(System.String));
                                        columns.Add("EKBE_DMBTR_POP", typeof(System.String));
                                        columns.Add("EKBE_ELIKZ", typeof(System.String));
                                        columns.Add("EKBE_EMATN", typeof(System.String));
                                        columns.Add("EKBE_ERNAM", typeof(System.String));
                                        columns.Add("EKBE_ET_UPD", typeof(System.String));
                                        columns.Add("EKBE_ETENS", typeof(System.String));
                                        columns.Add("EKBE_EVERE", typeof(System.String));
                                        columns.Add("EKBE_GJAHR", typeof(System.String));
                                        columns.Add("EKBE_GRUND", typeof(System.String));
                                        columns.Add("EKBE_HSWAE", typeof(System.String));
                                        columns.Add("EKBE_INTROW", typeof(System.String));
                                        columns.Add("EKBE_KNUMV", typeof(System.String));
                                        columns.Add("EKBE_KUDIF", typeof(System.String));
                                        columns.Add("EKBE_LEMIN", typeof(System.String));
                                        columns.Add("EKBE_LFBNR", typeof(System.String));
                                        columns.Add("EKBE_LFGJA", typeof(System.String));
                                        columns.Add("EKBE_LFPOS", typeof(System.String));
                                        columns.Add("EKBE_LSMEH", typeof(System.String));
                                        columns.Add("EKBE_LSMNG", typeof(System.String));
                                        columns.Add("EKBE_MATNR", typeof(System.String));
                                        columns.Add("EKBE_MENGE", typeof(System.String));
                                        columns.Add("EKBE_MENGE_POP", typeof(System.String));
                                        columns.Add("EKBE_MWSKZ", typeof(System.String));
                                        columns.Add("EKBE_PACKNO", typeof(System.String));
                                        columns.Add("EKBE_REEWR", typeof(System.String));
                                        columns.Add("EKBE_REFWR", typeof(System.String));
                                        columns.Add("EKBE_RETAMT_FC", typeof(System.String));
                                        columns.Add("EKBE_RETAMT_LC", typeof(System.String));
                                        columns.Add("EKBE_RETAMTP_FC", typeof(System.String));
                                        columns.Add("EKBE_RETAMTP_LC", typeof(System.String));
                                        columns.Add("EKBE_REWRB", typeof(System.String));
                                        columns.Add("EKBE_SAPRL", typeof(System.String));
                                        columns.Add("EKBE_SHKZG", typeof(System.String));
                                        columns.Add("EKBE_SRVPOS", typeof(System.String));
                                        columns.Add("EKBE_VGABE", typeof(System.String));
                                        columns.Add("EKBE_WAERS", typeof(System.String));
                                        columns.Add("EKBE_WEORA", typeof(System.String));
                                        columns.Add("EKBE_WERKS", typeof(System.String));
                                        columns.Add("EKBE_WESBB", typeof(System.String));
                                        columns.Add("EKBE_WESBS", typeof(System.String));
                                        columns.Add("EKBE_WKURS", typeof(System.String));
                                        columns.Add("EKBE_WRBTR", typeof(System.String));
                                        columns.Add("EKBE_WRBTR_POP", typeof(System.String));
                                        columns.Add("EKBE_XBLNR", typeof(System.String));
                                        columns.Add("EKBE_XMACC", typeof(System.String));
                                        columns.Add("EKBE_XUNPL", typeof(System.String));
                                        columns.Add("EKBE_XWOFF", typeof(System.String));
                                        columns.Add("EKBE_XWSBR", typeof(System.String));
                                        columns.Add("EKBE_ZEKKN", typeof(System.String));
                                        columns.Add("EKBE_ERUSERNAME", typeof(System.String));
                                        columns.Add("EKBE_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKBE_UDATE", typeof(System.String));
                                        columns.Add("EKBE_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- PO_InfRecGe_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PO_InfRecGe.txt"))
                                    {
                                        columns.Add("EINA_SYSID", typeof(System.String));
                                        columns.Add("EINA_MANDT", typeof(System.String));
                                        columns.Add("EINA_INFNR", typeof(System.String));
                                        columns.Add("EINA_ANZPU", typeof(System.String));
                                        columns.Add("EINA_ERDAT", typeof(System.String));
                                        columns.Add("EINA_ERNAM", typeof(System.String));
                                        columns.Add("EINA_IDNLF", typeof(System.String));
                                        columns.Add("EINA_KOLIF", typeof(System.String));
                                        columns.Add("EINA_LIFAB", typeof(System.String));
                                        columns.Add("EINA_LIFBI", typeof(System.String));
                                        columns.Add("EINA_LIFNR", typeof(System.String));
                                        columns.Add("EINA_LMEIN", typeof(System.String));
                                        columns.Add("EINA_LOEKZ", typeof(System.String));
                                        columns.Add("EINA_LTSNR", typeof(System.String));
                                        columns.Add("EINA_LTSSF", typeof(System.String));
                                        columns.Add("EINA_MAHN1", typeof(System.String));
                                        columns.Add("EINA_MAHN2", typeof(System.String));
                                        columns.Add("EINA_MAHN3", typeof(System.String));
                                        columns.Add("EINA_MATKL", typeof(System.String));
                                        columns.Add("EINA_MATNR", typeof(System.String));
                                        columns.Add("EINA_MEINS", typeof(System.String));
                                        columns.Add("EINA_MFRNR", typeof(System.String));
                                        columns.Add("EINA_PUNEI", typeof(System.String));
                                        columns.Add("EINA_REGIO", typeof(System.String));
                                        columns.Add("EINA_RELIF", typeof(System.String));
                                        columns.Add("EINA_RUECK", typeof(System.String));
                                        columns.Add("EINA_SORTL", typeof(System.String));
                                        columns.Add("EINA_TELF1", typeof(System.String));
                                        columns.Add("EINA_TXZ01", typeof(System.String));
                                        columns.Add("EINA_UMREN", typeof(System.String));
                                        columns.Add("EINA_UMREZ", typeof(System.String));
                                        columns.Add("EINA_URZDT", typeof(System.String));
                                        columns.Add("EINA_URZLA", typeof(System.String));
                                        columns.Add("EINA_URZNR", typeof(System.String));
                                        columns.Add("EINA_URZTP", typeof(System.String));
                                        columns.Add("EINA_URZZT", typeof(System.String));
                                        columns.Add("EINA_VABME", typeof(System.String));
                                        columns.Add("EINA_VERKF", typeof(System.String));
                                        columns.Add("EINA_WGLIF", typeof(System.String));
                                        columns.Add("EINA_ERUSERNAME", typeof(System.String));
                                        columns.Add("EINA_AEUSERNAME", typeof(System.String));
                                        columns.Add("EINA_UDATE", typeof(System.String));
                                        columns.Add("EINA_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- PO_InfRecPOOrg_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PO_InfRecPOOrg.txt"))
                                    {
                                        columns.Add("EINE_SYSID", typeof(System.String));
                                        columns.Add("EINE_MANDT", typeof(System.String));
                                        columns.Add("EINE_INFNR", typeof(System.String));
                                        columns.Add("EINE_EKORG", typeof(System.String));
                                        columns.Add("EINE_ESOKZ", typeof(System.String));
                                        columns.Add("EINE_WERKS", typeof(System.String));
                                        columns.Add("EINE_ERDAT", typeof(System.String));
                                        columns.Add("EINE_ERNAM", typeof(System.String));
                                        columns.Add("EINE_EKGRP", typeof(System.String));
                                        columns.Add("EINE_WAERS", typeof(System.String));
                                        columns.Add("EINE_BONUS", typeof(System.String));
                                        columns.Add("EINE_MGBON", typeof(System.String));
                                        columns.Add("EINE_MINBM", typeof(System.String));
                                        columns.Add("EINE_NORBM", typeof(System.String));
                                        columns.Add("EINE_BSTMA", typeof(System.String));
                                        columns.Add("EINE_APLFZ", typeof(System.String));
                                        columns.Add("EINE_EBELN", typeof(System.String));
                                        columns.Add("EINE_EBELP", typeof(System.String));
                                        columns.Add("EINE_DATLB", typeof(System.String));
                                        columns.Add("EINE_ANGNR", typeof(System.String));
                                        columns.Add("EINE_ANGDT", typeof(System.String));
                                        columns.Add("EINE_ANFNR", typeof(System.String));
                                        columns.Add("EINE_ANFPS", typeof(System.String));
                                        columns.Add("EINE_ABSKZ", typeof(System.String));
                                        columns.Add("EINE_UNTTO", typeof(System.String));
                                        columns.Add("EINE_UEBTK", typeof(System.String));
                                        columns.Add("EINE_UEBTO", typeof(System.String));
                                        columns.Add("EINE_BSTYP", typeof(System.String));
                                        columns.Add("EINE_BWTAR", typeof(System.String));
                                        columns.Add("EINE_LOEKZ", typeof(System.String));
                                        columns.Add("EINE_MHDRZ", typeof(System.String));
                                        columns.Add("EINE_NETPR", typeof(System.String));
                                        columns.Add("EINE_PEINH", typeof(System.String));
                                        columns.Add("EINE_BPRME", typeof(System.String));
                                        columns.Add("EINE_PRDAT", typeof(System.String));
                                        columns.Add("EINE_KZABS", typeof(System.String));
                                        columns.Add("EINE_BPUMZ", typeof(System.String));
                                        columns.Add("EINE_BPUMN", typeof(System.String));
                                        columns.Add("EINE_INCO1", typeof(System.String));
                                        columns.Add("EINE_INCO2", typeof(System.String));
                                        columns.Add("EINE_ERUSERNAME", typeof(System.String));
                                        columns.Add("EINE_AEUSERNAME", typeof(System.String));
                                        columns.Add("EINE_UDATE", typeof(System.String));
                                        columns.Add("EINE_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- PO_Item_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PO_Item.txt"))
                                    {
                                        columns.Add("EKPO_SYSID", typeof(System.String));
                                        columns.Add("EKPO_MANDT", typeof(System.String));
                                        columns.Add("EKPO_BUKRS", typeof(System.String));
                                        columns.Add("EKPO_EBELN", typeof(System.String));
                                        columns.Add("EKPO_EBELP", typeof(System.String));
                                        columns.Add("EKPO_WERKS", typeof(System.String));
                                        columns.Add("EKPO_LOEKZ", typeof(System.String));
                                        columns.Add("EKPO_PSTYP", typeof(System.String));
                                        columns.Add("EKPO_UDATE", typeof(System.String));
                                        columns.Add("EKPO_ORDERDATE", typeof(System.String));
                                        columns.Add("EKPO_MENGE", typeof(System.String));
                                        columns.Add("EKPO_MEINS", typeof(System.String));
                                        columns.Add("EKPO_NETWR", typeof(System.String));
                                        columns.Add("EKPO_CAMOUNT", typeof(System.String));
                                        columns.Add("EKPO_EURAMOUNT", typeof(System.String));
                                        columns.Add("EKPO_WKURS", typeof(System.String));
                                        columns.Add("EKPO_RATE", typeof(System.String));
                                        columns.Add("EKPO_LONGTEXT", typeof(System.String));
                                        columns.Add("EKPO_TXZ01", typeof(System.String));
                                        columns.Add("EKPO_MATKL", typeof(System.String));
                                        columns.Add("EKPO_MATNR", typeof(System.String));
                                        columns.Add("EKPO_REVLV", typeof(System.String));
                                        columns.Add("EKPO_IDNLF", typeof(System.String));
                                        columns.Add("EKPO_BANFN", typeof(System.String));
                                        columns.Add("EKPO_BNFPO", typeof(System.String));
                                        columns.Add("EKPO_AFNAM", typeof(System.String));
                                        columns.Add("EKPO_BEDNR", typeof(System.String));
                                        columns.Add("EKPO_KONNR", typeof(System.String));
                                        columns.Add("EKPO_KTPNR", typeof(System.String));
                                        columns.Add("EKPO_WEPOS", typeof(System.String));
                                        columns.Add("EKPO_EVERS", typeof(System.String));
                                        columns.Add("EKPO_INSMK", typeof(System.String));
                                        columns.Add("EKPO_MHDRZ", typeof(System.String));
                                        columns.Add("EKPO_IPRKZ", typeof(System.String));
                                        columns.Add("EKPO_SSQSS", typeof(System.String));
                                        columns.Add("EKPO_PLIFZ", typeof(System.String));
                                        columns.Add("EKPO_WEBAZ", typeof(System.String));
                                        columns.Add("EKPO_LEWED", typeof(System.String));
                                        columns.Add("EKPO_ABLAD", typeof(System.String));
                                        columns.Add("EKPO_LGORT", typeof(System.String));
                                        columns.Add("EKPO_NAME1", typeof(System.String));
                                        columns.Add("EKPO_NAME2", typeof(System.String));
                                        columns.Add("EKPO_STREET", typeof(System.String));
                                        columns.Add("EKPO_POST_CODE1", typeof(System.String));
                                        columns.Add("EKPO_CITY1", typeof(System.String));
                                        columns.Add("EKPO_REGION", typeof(System.String));
                                        columns.Add("EKPO_COUNTRY", typeof(System.String));
                                        columns.Add("EKPO_PO_BOX", typeof(System.String));
                                        columns.Add("EKPO_KZABS", typeof(System.String));
                                        columns.Add("EKPO_LABNR", typeof(System.String));
                                        columns.Add("EKPO_ELIKZ", typeof(System.String));
                                        columns.Add("EKPO_EREKZ", typeof(System.String));
                                        columns.Add("EKPO_UEBTO", typeof(System.String));
                                        columns.Add("EKPO_UEBTK", typeof(System.String));
                                        columns.Add("EKPO_UNTTO", typeof(System.String));
                                        columns.Add("EKPO_XOBLR", typeof(System.String));
                                        columns.Add("EKPO_KNTTP", typeof(System.String));
                                        columns.Add("EKPO_VRTKZ", typeof(System.String));
                                        columns.Add("EKPO_INCO1", typeof(System.String));
                                        columns.Add("EKPO_INCO2", typeof(System.String));
                                        columns.Add("EKPO_MFRNR", typeof(System.String));
                                        columns.Add("EKPO_MFRPN", typeof(System.String));
                                        columns.Add("EKPO_EAN11", typeof(System.String));
                                        columns.Add("EKPO_ERUSERNAME", typeof(System.String));
                                        columns.Add("EKPO_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKPO_AEDAT", typeof(System.String));
                                        columns.Add("EKPO_DATUM", typeof(System.String));
                                        columns.Add("EKPO_ZZBP1", typeof(System.String));
                                        columns.Add("EKPO_ZZCR1", typeof(System.String));
                                        columns.Add("EKPO_ZZPT1", typeof(System.String));
                                        columns.Add("EKPO_ZZPD1", typeof(System.String));
                                        columns.Add("EKPO_ZZBP2", typeof(System.String));
                                        columns.Add("EKPO_ZZCR2", typeof(System.String));
                                        columns.Add("EKPO_ZZPT2", typeof(System.String));
                                        columns.Add("EKPO_ZZPD2", typeof(System.String));
                                        columns.Add("EKPO_ZZBP3", typeof(System.String));
                                        columns.Add("EKPO_ZZCR3", typeof(System.String));
                                        columns.Add("EKPO_ZZPT3", typeof(System.String));
                                        columns.Add("EKPO_ZZPD3", typeof(System.String));
                                        columns.Add("EKPO_ZZBP4", typeof(System.String));
                                        columns.Add("EKPO_ZZCR4", typeof(System.String));
                                        columns.Add("EKPO_ZZPT4", typeof(System.String));
                                        columns.Add("EKPO_ZZPD4", typeof(System.String));
                                        columns.Add("EKPO_ZZBP5", typeof(System.String));
                                        columns.Add("EKPO_ZZCR5", typeof(System.String));
                                        columns.Add("EKPO_ZZPT5", typeof(System.String));
                                        columns.Add("EKPO_ZZPD5", typeof(System.String));
                                        columns.Add("EKPO_WAERS", typeof(System.String));
                                        columns.Add("EKPO_ADRNR", typeof(System.String));
                                        columns.Add("EKPO_ADRN2", typeof(System.String));


                                    }
                                    //------------------------------- PO_Schedule_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PO_Schedule.txt"))
                                    {
                                        columns.Add("EKET_SYSID", typeof(System.String));
                                        columns.Add("EKET_MANDT", typeof(System.String));
                                        columns.Add("EKET_EBELN", typeof(System.String));
                                        columns.Add("EKET_EBELP", typeof(System.String));
                                        columns.Add("EKET_ETENR", typeof(System.String));
                                        columns.Add("EKET_ESTKZ", typeof(System.String));
                                        columns.Add("EKET_CHARG", typeof(System.String));
                                        columns.Add("EKET_LICHA", typeof(System.String));
                                        columns.Add("EKET_BEDAT", typeof(System.String));
                                        columns.Add("EKET_EINDT", typeof(System.String));
                                        columns.Add("EKET_SLFDT", typeof(System.String));
                                        columns.Add("EKET_MENGE", typeof(System.String));
                                        columns.Add("EKET_WEMNG", typeof(System.String));
                                        columns.Add("EKET_DABMG", typeof(System.String));
                                        columns.Add("EKET_BANFN", typeof(System.String));
                                        columns.Add("EKET_BNFPO", typeof(System.String));
                                        columns.Add("EKET_MAHNZ", typeof(System.String));
                                        columns.Add("EKET_GLMNG", typeof(System.String));
                                        columns.Add("EKET_AMENG", typeof(System.String));
                                        columns.Add("EKET_WAMNG", typeof(System.String));
                                        columns.Add("EKET_FIXKZ", typeof(System.String));
                                        columns.Add("EKET_ERUSERNAME", typeof(System.String));
                                        columns.Add("EKET_AEUSERNAME", typeof(System.String));
                                        columns.Add("EKET_UDATE", typeof(System.String));
                                        columns.Add("EKET_DATUM", typeof(System.String));

                                    }
                                    //------------------------------- PR_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PR.txt"))
                                    {
                                        columns.Add("EBAN_SYSID", typeof(System.String));
                                        columns.Add("EBAN_MANDT", typeof(System.String));
                                        columns.Add("EBAN_BANFN", typeof(System.String));
                                        columns.Add("EBAN_BNFPO", typeof(System.String));
                                        columns.Add("EBAN_BSART", typeof(System.String));
                                        columns.Add("EBAN_PSTYP", typeof(System.String));
                                        columns.Add("EBAN_LOEKZ", typeof(System.String));
                                        columns.Add("EBAN_WERKS", typeof(System.String));
                                        columns.Add("EBAN_BADAT", typeof(System.String));
                                        columns.Add("EBAN_FRGDT", typeof(System.String));
                                        columns.Add("EBAN_MENGE", typeof(System.String));
                                        columns.Add("EBAN_MEINS", typeof(System.String));
                                        columns.Add("EBAN_PREIS", typeof(System.String));
                                        columns.Add("EBAN_AMOUNT", typeof(System.String));
                                        columns.Add("EBAN_RLWRT", typeof(System.String));
                                        columns.Add("EBAN_WAERS", typeof(System.String));
                                        columns.Add("EBAN_RATE", typeof(System.String));
                                        columns.Add("EBAN_EURAMOUNT", typeof(System.String));
                                        columns.Add("EBAN_TXZ01", typeof(System.String));
                                        columns.Add("EBAN_MATKL", typeof(System.String));
                                        columns.Add("EBAN_MATNR", typeof(System.String));
                                        columns.Add("EBAN_REVLV", typeof(System.String));
                                        columns.Add("EBAN_KONNR", typeof(System.String));
                                        columns.Add("EBAN_KTPNR", typeof(System.String));
                                        columns.Add("EBAN_LIFNR", typeof(System.String));
                                        columns.Add("EBAN_FLIEF", typeof(System.String));
                                        columns.Add("EBAN_POSID", typeof(System.String));
                                        columns.Add("EBAN_ZZBKZ", typeof(System.String));
                                        columns.Add("EBAN_PRCTR", typeof(System.String));
                                        columns.Add("EBAN_AFNAM", typeof(System.String));
                                        columns.Add("EBAN_ESTKZ", typeof(System.String));
                                        columns.Add("EBAN_STATU", typeof(System.String));
                                        columns.Add("EBAN_DISPO", typeof(System.String));
                                        columns.Add("EBAN_FRGKZ", typeof(System.String));
                                        columns.Add("EBAN_FRGZU", typeof(System.String));
                                        columns.Add("EBAN_FRGST", typeof(System.String));
                                        columns.Add("EBAN_KNTTP", typeof(System.String));
                                        columns.Add("EBAN_EKORG", typeof(System.String));
                                        columns.Add("EBAN_EKGRP", typeof(System.String));
                                        columns.Add("EBAN_ERNAM", typeof(System.String));
                                        columns.Add("EBAN_ERUSERNAME", typeof(System.String));
                                        columns.Add("EBAN_ERDAT", typeof(System.String));
                                        columns.Add("EBAN_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- PR_Accounting_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PR_Accounting.txt"))
                                    {
                                        columns.Add("EBKN_SYSID", typeof(System.String));
                                        columns.Add("EBKN_MANDT", typeof(System.String));
                                        columns.Add("EBKN_BANFN", typeof(System.String));
                                        columns.Add("EBKN_BNFPO", typeof(System.String));
                                        columns.Add("EBKN_ZEBKN", typeof(System.String));
                                        columns.Add("EBKN_LOEKZ", typeof(System.String));
                                        columns.Add("EBKN_ERDAT", typeof(System.String));
                                        columns.Add("EBKN_ERNAM", typeof(System.String));
                                        columns.Add("EBKN_MENGE", typeof(System.String));
                                        columns.Add("EBKN_VPROZ", typeof(System.String));
                                        columns.Add("EBKN_SAKTO", typeof(System.String));
                                        columns.Add("EBKN_KOSTL", typeof(System.String));
                                        columns.Add("EBKN_VBELN", typeof(System.String));
                                        columns.Add("EBKN_VBELP", typeof(System.String));
                                        columns.Add("EBKN_VETEN", typeof(System.String));
                                        columns.Add("EBKN_ANLN1", typeof(System.String));
                                        columns.Add("EBKN_ANLN2", typeof(System.String));
                                        columns.Add("EBKN_AUFNR", typeof(System.String));
                                        columns.Add("EBKN_WEMPF", typeof(System.String));
                                        columns.Add("EBKN_ABLAD", typeof(System.String));
                                        columns.Add("EBKN_KOKRS", typeof(System.String));
                                        columns.Add("EBKN_KSTRG", typeof(System.String));
                                        columns.Add("EBKN_PRCTR", typeof(System.String));
                                        columns.Add("EBKN_PS_PSP_PNR", typeof(System.String));
                                        columns.Add("EBKN_NPLNR", typeof(System.String));
                                        columns.Add("EBKN_AUFPL", typeof(System.String));
                                        columns.Add("EBKN_APLZL", typeof(System.String));
                                        columns.Add("EBKN_DABRZ", typeof(System.String));
                                        columns.Add("EBKN_NETWR", typeof(System.String));
                                        columns.Add("EBKN_UDATE", typeof(System.String));
                                        columns.Add("EBKN_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- ProdOrder_Header_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("ProdOrder_Header.txt"))
                                    {
                                        columns.Add("AFKO_SYSID", typeof(System.String));
                                        columns.Add("AFKO_MANDT", typeof(System.String));
                                        columns.Add("AFKO_AUFNR", typeof(System.String));
                                        columns.Add("AFKO_GSTRP", typeof(System.String));
                                        columns.Add("AFKO_GLTRP", typeof(System.String));
                                        columns.Add("AFKO_GSTRS", typeof(System.String));
                                        columns.Add("AFKO_FTRMS", typeof(System.String));
                                        columns.Add("AFKO_GLTRS", typeof(System.String));
                                        columns.Add("AFKO_GSTRI", typeof(System.String));
                                        columns.Add("AFKO_FTRMI", typeof(System.String));
                                        columns.Add("AFKO_GETRI", typeof(System.String));
                                        columns.Add("AFKO_GLTRI", typeof(System.String));
                                        columns.Add("AFKO_RSNUM", typeof(System.String));
                                        columns.Add("AFKO_PLNNR", typeof(System.String));
                                        columns.Add("AFKO_PLNAW", typeof(System.String));
                                        columns.Add("AFKO_AUFPL", typeof(System.String));
                                        columns.Add("AFKO_PRONR", typeof(System.String));
                                        columns.Add("AFKO_NTZUE", typeof(System.String));
                                        columns.Add("AFKO_RMANR", typeof(System.String));
                                        columns.Add("AFKO_ERDATE", typeof(System.String));
                                        columns.Add("AFKO_UDATE", typeof(System.String));
                                        columns.Add("AFKO_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- ProdOrder_Item_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("ProdOrder_Item.txt"))
                                    {
                                        columns.Add("AFPO_SYSID", typeof(System.String));
                                        columns.Add("AFPO_MANDT", typeof(System.String));
                                        columns.Add("AFPO_AUFNR", typeof(System.String));
                                        columns.Add("AFPO_POSNR", typeof(System.String));
                                        columns.Add("AFPO_SOBES", typeof(System.String));
                                        columns.Add("AFPO_PROJN", typeof(System.String));
                                        columns.Add("AFPO_KDAUF", typeof(System.String));
                                        columns.Add("AFPO_KDPOS", typeof(System.String));
                                        columns.Add("AFPO_PSAMG", typeof(System.String));
                                        columns.Add("AFPO_PSMNG", typeof(System.String));
                                        columns.Add("AFPO_WEMNG", typeof(System.String));
                                        columns.Add("AFPO_IAMNG", typeof(System.String));
                                        columns.Add("AFPO_MEINS", typeof(System.String));
                                        columns.Add("AFPO_MATNR", typeof(System.String));
                                        columns.Add("AFPO_LTRMI", typeof(System.String));
                                        columns.Add("AFPO_DWERK", typeof(System.String));
                                        columns.Add("AFPO_DAUTY", typeof(System.String));
                                        columns.Add("AFPO_DAUAT", typeof(System.String));
                                        columns.Add("AFPO_DGLTP", typeof(System.String));
                                        columns.Add("AFPO_DGLTS", typeof(System.String));
                                        columns.Add("AFPO_SERNP", typeof(System.String));
                                        columns.Add("AFPO_XLOEK", typeof(System.String));
                                        columns.Add("AFPO_ERDATE", typeof(System.String));
                                        columns.Add("AFPO_UDATE", typeof(System.String));
                                        columns.Add("AFPO_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- MD_ProfitCenter_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("ProfitCenter.txt"))
                                    {
                                        columns.Add("CEPC_SYSID", typeof(System.String));
                                        columns.Add("CEPC_MANDT", typeof(System.String));
                                        columns.Add("CEPC_KOKRS", typeof(System.String));
                                        columns.Add("CEPC_PRCTR", typeof(System.String));
                                        columns.Add("CEPC_SPRAS", typeof(System.String));
                                        columns.Add("CEPC_KTEXT", typeof(System.String));
                                        columns.Add("CEPC_LTEXT", typeof(System.String));
                                        columns.Add("CEPC_VERAK", typeof(System.String));
                                        columns.Add("CEPC_ABTEI", typeof(System.String));
                                        columns.Add("CEPC_STATUS", typeof(System.String));
                                        columns.Add("CEPC_KHINR", typeof(System.String));
                                        columns.Add("CEPC_HITXT", typeof(System.String));
                                        columns.Add("CEPC_DATAB", typeof(System.String));
                                        columns.Add("CEPC_DATBI", typeof(System.String));
                                        columns.Add("CEPC_ERSDA", typeof(System.String));
                                        columns.Add("CEPC_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- MD_Project_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("Project.txt"))
                                    {
                                        columns.Add("PROJ_SYSID", typeof(System.String));
                                        columns.Add("PROJ_MANDT", typeof(System.String));
                                        columns.Add("PROJ_VBUKR", typeof(System.String));
                                        columns.Add("PROJ_PSPID", typeof(System.String));
                                        columns.Add("PROJ_POST1", typeof(System.String));
                                        columns.Add("PROJ_PLFAZ", typeof(System.String));
                                        columns.Add("PROJ_PLSEZ", typeof(System.String));
                                        columns.Add("PROJ_VERNA", typeof(System.String));
                                        columns.Add("PROJ_ASTNA", typeof(System.String));
                                        columns.Add("PROJ_ISTAT", typeof(System.String));
                                        columns.Add("PROJ_SPRAS", typeof(System.String));
                                        columns.Add("PROJ_TXT30", typeof(System.String));
                                        columns.Add("PROJ_PRCTR", typeof(System.String));
                                        columns.Add("PROJ_POSID", typeof(System.String));
                                        columns.Add("PROJ_PSPNR", typeof(System.String));
                                        columns.Add("PROJ_PRPS_POST1", typeof(System.String));
                                        columns.Add("PROJ_ERDAT", typeof(System.String));
                                        columns.Add("PROJ_AEDAT", typeof(System.String));
                                        columns.Add("PROJ_DATUM", typeof(System.String));
                                        columns.Add("PROJ_ZZBKZ", typeof(System.String));
                                    }
                                    //------------------------------- MD_PurchasingGroup_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("PurchasingGroup.txt"))
                                    {
                                        columns.Add("T024_SYSID", typeof(System.String));
                                        columns.Add("T024_MANDT", typeof(System.String));
                                        columns.Add("T024_EKGRP", typeof(System.String));
                                        columns.Add("T024_EKNAM", typeof(System.String));
                                        columns.Add("T024_TELFX", typeof(System.String));
                                        columns.Add("T024_TEL_NUMBER", typeof(System.String));
                                        columns.Add("T024_TEL_EXTENS", typeof(System.String));
                                        columns.Add("T024_TITLE", typeof(System.String));
                                        columns.Add("T024_ZZ_DNAME", typeof(System.String));
                                        columns.Add("T024_ZZ_TNAME", typeof(System.String));
                                        columns.Add("T024_ZZ_ABTLG", typeof(System.String));
                                        columns.Add("T024_ZZ_STBEZ", typeof(System.String));
                                        columns.Add("T024_SMTP_ADDR", typeof(System.String));
                                        columns.Add("T024_EKTEL", typeof(System.String));
                                        columns.Add("T024_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- QInfRec_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("QInfRec.txt"))
                                    {
                                        columns.Add("QINF_SYSID", typeof(System.String));
                                        columns.Add("QINF_MANDT", typeof(System.String));
                                        columns.Add("QINF_MATNR", typeof(System.String));
                                        columns.Add("QINF_ZAEHL", typeof(System.String));
                                        columns.Add("QINF_REVLV", typeof(System.String));
                                        columns.Add("QINF_AENDERDAT", typeof(System.String));
                                        columns.Add("QINF_AENDERER", typeof(System.String));
                                        columns.Add("QINF_BEST_MG", typeof(System.String));
                                        columns.Add("QINF_CERTCONTROL", typeof(System.String));
                                        columns.Add("QINF_DAT_RUECK", typeof(System.String));
                                        columns.Add("QINF_ERSTELLDAT", typeof(System.String));
                                        columns.Add("QINF_ERSTELLER", typeof(System.String));
                                        columns.Add("QINF_FREI_DAT", typeof(System.String));
                                        columns.Add("QINF_FREI_MGKZ", typeof(System.String));
                                        columns.Add("QINF_FREI_MNG", typeof(System.String));
                                        columns.Add("QINF_LIEFERANT", typeof(System.String));
                                        columns.Add("QINF_LOEKZ", typeof(System.String));
                                        columns.Add("QINF_LTEXTKZ", typeof(System.String));
                                        columns.Add("QINF_ME", typeof(System.String));
                                        columns.Add("QINF_NOINSP", typeof(System.String));
                                        columns.Add("QINF_NOINSPABN", typeof(System.String));
                                        columns.Add("QINF_OBJNR", typeof(System.String));
                                        columns.Add("QINF_PLOS", typeof(System.String));
                                        columns.Add("QINF_PLOS2", typeof(System.String));
                                        columns.Add("QINF_QSSYSDAT", typeof(System.String));
                                        columns.Add("QINF_QSSYSFAM", typeof(System.String));
                                        columns.Add("QINF_QVVORH", typeof(System.String));
                                        columns.Add("QINF_SPERRFKT", typeof(System.String));
                                        columns.Add("QINF_SPERRGRUND", typeof(System.String));
                                        columns.Add("QINF_SPRACHE", typeof(System.String));
                                        columns.Add("QINF_SPRAS", typeof(System.String));
                                        columns.Add("QINF_STSMA", typeof(System.String));
                                        columns.Add("QINF_VARIABNAHM", typeof(System.String));
                                        columns.Add("QINF_VORLABN", typeof(System.String));
                                        columns.Add("QINF_WERK", typeof(System.String));
                                        columns.Add("QINF_LFA1_SPERQ", typeof(System.String));
                                        columns.Add("QINF_LFA1_QSSYS", typeof(System.String));
                                        columns.Add("QINF_LFA1_QSSYSDAT", typeof(System.String));
                                        columns.Add("QINF_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- QN_Action_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("QN_Action.txt"))
                                    {
                                        columns.Add("QMMA_SYSID", typeof(System.String));
                                        columns.Add("QMMA_MANDT", typeof(System.String));
                                        columns.Add("QMMA_QMNUM", typeof(System.String));
                                        columns.Add("QMMA_FENUM", typeof(System.String));
                                        columns.Add("QMMA_AEDAT", typeof(System.String));
                                        columns.Add("QMMA_AENAM", typeof(System.String));
                                        columns.Add("QMMA_AEZEIT", typeof(System.String));
                                        columns.Add("QMMA_AUTKZ", typeof(System.String));
                                        columns.Add("QMMA_ERDAT", typeof(System.String));
                                        columns.Add("QMMA_ERNAM", typeof(System.String));
                                        columns.Add("QMMA_ERZEIT", typeof(System.String));
                                        columns.Add("QMMA_FUNKTION", typeof(System.String));
                                        columns.Add("QMMA_INDTX", typeof(System.String));
                                        columns.Add("QMMA_KLAKZ", typeof(System.String));
                                        columns.Add("QMMA_KZACTIONBOX", typeof(System.String));
                                        columns.Add("QMMA_KZLOESCH", typeof(System.String));
                                        columns.Add("QMMA_KZMLA", typeof(System.String));
                                        columns.Add("QMMA_MAKLS", typeof(System.String));
                                        columns.Add("QMMA_MANUM", typeof(System.String));
                                        columns.Add("QMMA_MATXT", typeof(System.String));
                                        columns.Add("QMMA_MNCOD", typeof(System.String));
                                        columns.Add("QMMA_MNGFA", typeof(System.String));
                                        columns.Add("QMMA_MNGRP", typeof(System.String));
                                        columns.Add("QMMA_MNKAT", typeof(System.String));
                                        columns.Add("QMMA_MNVER", typeof(System.String));
                                        columns.Add("QMMA_PETER", typeof(System.String));
                                        columns.Add("QMMA_PETUR", typeof(System.String));
                                        columns.Add("QMMA_PSTER", typeof(System.String));
                                        columns.Add("QMMA_PSTUR", typeof(System.String));
                                        columns.Add("QMMA_QMANUM", typeof(System.String));
                                        columns.Add("QMMA_URNUM", typeof(System.String));
                                        columns.Add("QMMA_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- QN_Header_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("QN_Header.txt"))
                                    {
                                        columns.Add("QMEL_SYSID", typeof(System.String));
                                        columns.Add("QMEL_MANDT", typeof(System.String));
                                        columns.Add("QMEL_QMNUM", typeof(System.String));
                                        columns.Add("QMEL_QMART", typeof(System.String));
                                        columns.Add("QMEL_QMTXT", typeof(System.String));
                                        columns.Add("QMEL_ERNAM", typeof(System.String));
                                        columns.Add("QMEL_ERDAT", typeof(System.String));
                                        columns.Add("QMEL_AENAM", typeof(System.String));
                                        columns.Add("QMEL_AEDAT", typeof(System.String));
                                        columns.Add("QMEL_QMDAT", typeof(System.String));
                                        columns.Add("QMEL_QMNAM", typeof(System.String));
                                        columns.Add("QMEL_AUFNR", typeof(System.String));
                                        columns.Add("QMEL_VBELN", typeof(System.String));
                                        columns.Add("QMEL_MAWERK", typeof(System.String));
                                        columns.Add("QMEL_PRUEFLOS", typeof(System.String));
                                        columns.Add("QMEL_CHARG", typeof(System.String));
                                        columns.Add("QMEL_EMATNR", typeof(System.String));
                                        columns.Add("QMEL_EBELN", typeof(System.String));
                                        columns.Add("QMEL_EBELP", typeof(System.String));
                                        columns.Add("QMEL_MJAHR", typeof(System.String));
                                        columns.Add("QMEL_MBLNR", typeof(System.String));
                                        columns.Add("QMEL_MBLPO", typeof(System.String));
                                        columns.Add("QMEL_LS_KDAUF", typeof(System.String));
                                        columns.Add("QMEL_LS_KDPOS", typeof(System.String));
                                        columns.Add("QMEL_LS_VBELN", typeof(System.String));
                                        columns.Add("QMEL_LS_POSNR", typeof(System.String));
                                        columns.Add("QMEL_MGEIG", typeof(System.String));
                                        columns.Add("QMEL_MGFRD", typeof(System.String));
                                        columns.Add("QMEL_BZMNG", typeof(System.String));
                                        columns.Add("QMEL_RKMNG", typeof(System.String));
                                        columns.Add("QMEL_RGMNG", typeof(System.String));
                                        columns.Add("QMEL_RKDAT", typeof(System.String));
                                        columns.Add("QMEL_KDMAT", typeof(System.String));
                                        columns.Add("QMEL_IDNLF", typeof(System.String));
                                        columns.Add("QMEL_SERIALNR", typeof(System.String));
                                        columns.Add("QMEL_KZLOESCH", typeof(System.String));
                                        columns.Add("QMEL_ZZ_QFART", typeof(System.String));
                                        columns.Add("QMEL_ZZ_DELIVERY", typeof(System.String));
                                        columns.Add("QMEL_EKORG", typeof(System.String));
                                        columns.Add("QMEL_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- QN_Item_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("QN_Item.txt"))
                                    {
                                        columns.Add("QMFE_SYSID", typeof(System.String));
                                        columns.Add("QMFE_MANDT", typeof(System.String));
                                        columns.Add("QMFE_QMNUM", typeof(System.String));
                                        columns.Add("QMFE_FENUM", typeof(System.String));
                                        columns.Add("QMFE_AEDAT", typeof(System.String));
                                        columns.Add("QMFE_AENAM", typeof(System.String));
                                        columns.Add("QMFE_AEZEIT", typeof(System.String));
                                        columns.Add("QMFE_ANZFEHLER", typeof(System.String));
                                        columns.Add("QMFE_ARBPL", typeof(System.String));
                                        columns.Add("QMFE_ARBPLWERK", typeof(System.String));
                                        columns.Add("QMFE_AUTKZ", typeof(System.String));
                                        columns.Add("QMFE_BAUTL", typeof(System.String));
                                        columns.Add("QMFE_CROBJTY", typeof(System.String));
                                        columns.Add("QMFE_EBORT", typeof(System.String));
                                        columns.Add("QMFE_EKORG", typeof(System.String));
                                        columns.Add("QMFE_EQUNR", typeof(System.String));
                                        columns.Add("QMFE_ERDAT", typeof(System.String));
                                        columns.Add("QMFE_ERNAM", typeof(System.String));
                                        columns.Add("QMFE_ERZEIT", typeof(System.String));
                                        columns.Add("QMFE_FCOAUFNR", typeof(System.String));
                                        columns.Add("QMFE_FEART", typeof(System.String));
                                        columns.Add("QMFE_FECOD", typeof(System.String));
                                        columns.Add("QMFE_FEGRP", typeof(System.String));
                                        columns.Add("QMFE_FEHLBEW", typeof(System.String));
                                        columns.Add("QMFE_FEKAT", typeof(System.String));
                                        columns.Add("QMFE_FENUMORG", typeof(System.String));
                                        columns.Add("QMFE_FEQKLAS", typeof(System.String));
                                        columns.Add("QMFE_FETXT", typeof(System.String));
                                        columns.Add("QMFE_FEVER", typeof(System.String));
                                        columns.Add("QMFE_FMGEIG", typeof(System.String));
                                        columns.Add("QMFE_FMGEIN", typeof(System.String));
                                        columns.Add("QMFE_FMGFRD", typeof(System.String));
                                        columns.Add("QMFE_HERPOS", typeof(System.String));
                                        columns.Add("QMFE_INDTX", typeof(System.String));
                                        columns.Add("QMFE_INFNR", typeof(System.String));
                                        columns.Add("QMFE_KOSTL", typeof(System.String));
                                        columns.Add("QMFE_KZLOESCH", typeof(System.String));
                                        columns.Add("QMFE_KZMLA", typeof(System.String));
                                        columns.Add("QMFE_KZORG", typeof(System.String));
                                        columns.Add("QMFE_KZSYSFE", typeof(System.String));
                                        columns.Add("QMFE_LSTAR", typeof(System.String));
                                        columns.Add("QMFE_MATNR", typeof(System.String));
                                        columns.Add("QMFE_MENGE", typeof(System.String));
                                        columns.Add("QMFE_MERKNR", typeof(System.String));
                                        columns.Add("QMFE_OTEIL", typeof(System.String));
                                        columns.Add("QMFE_OTGRP", typeof(System.String));
                                        columns.Add("QMFE_OTKAT", typeof(System.String));
                                        columns.Add("QMFE_OTVER", typeof(System.String));
                                        columns.Add("QMFE_PHYNR", typeof(System.String));
                                        columns.Add("QMFE_PNLKN", typeof(System.String));
                                        columns.Add("QMFE_POSNR", typeof(System.String));
                                        columns.Add("QMFE_PROBENR", typeof(System.String));
                                        columns.Add("QMFE_PRUEFLINR", typeof(System.String));
                                        columns.Add("QMFE_PRZNR", typeof(System.String));
                                        columns.Add("QMFE_TPLNR", typeof(System.String));
                                        columns.Add("QMFE_UNITFLBEW", typeof(System.String));
                                        columns.Add("QMFE_WDFEH", typeof(System.String));
                                        columns.Add("QMFE_WERKS", typeof(System.String));
                                        columns.Add("QMFE_ZZ_MVA", typeof(System.String));
                                        columns.Add("QMFE_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- QN_Measure_ETL Table ------------------------------//
                                    else if (fileUploaded.Contains("QN_Measure.txt"))
                                    {
                                        columns.Add("QMSM_SYSID", typeof(System.String));
                                        columns.Add("QMSM_MANDT", typeof(System.String));
                                        columns.Add("QMSM_QMNUM", typeof(System.String));
                                        columns.Add("QMSM_MANUM", typeof(System.String));
                                        columns.Add("QMSM_MNKAT", typeof(System.String));
                                        columns.Add("QMSM_MNGRP", typeof(System.String));
                                        columns.Add("QMSM_MNCOD", typeof(System.String));
                                        columns.Add("QMSM_MNVER", typeof(System.String));
                                        columns.Add("QMSM_FOLGEACT", typeof(System.String));
                                        columns.Add("QMSM_FOLACTPROT", typeof(System.String));
                                        columns.Add("QMSM_MATXT", typeof(System.String));
                                        columns.Add("QMSM_ERNAM", typeof(System.String));
                                        columns.Add("QMSM_ERDAT", typeof(System.String));
                                        columns.Add("QMSM_AENAM", typeof(System.String));
                                        columns.Add("QMSM_AEDAT", typeof(System.String));
                                        columns.Add("QMSM_PSTER", typeof(System.String));
                                        columns.Add("QMSM_PETER", typeof(System.String));
                                        columns.Add("QMSM_OBJNR", typeof(System.String));
                                        columns.Add("QMSM_INDTX", typeof(System.String));
                                        columns.Add("QMSM_KZMLA", typeof(System.String));
                                        columns.Add("QMSM_PSTUR", typeof(System.String));
                                        columns.Add("QMSM_PETUR", typeof(System.String));
                                        columns.Add("QMSM_ERLNAM", typeof(System.String));
                                        columns.Add("QMSM_ERLDAT", typeof(System.String));
                                        columns.Add("QMSM_ERLZEIT", typeof(System.String));
                                        columns.Add("QMSM_WDVDAT", typeof(System.String));
                                        columns.Add("QMSM_FENUM", typeof(System.String));
                                        columns.Add("QMSM_URNUM", typeof(System.String));
                                        columns.Add("QMSM_ERZEIT", typeof(System.String));
                                        columns.Add("QMSM_AEZEIT", typeof(System.String));
                                        columns.Add("QMSM_PARVW", typeof(System.String));
                                        columns.Add("QMSM_PARNR", typeof(System.String));
                                        columns.Add("QMSM_MMENGE", typeof(System.String));
                                        columns.Add("QMSM_MMGEIN", typeof(System.String));
                                        columns.Add("QMSM_BAUTL", typeof(System.String));
                                        columns.Add("QMSM_KZLOESCH", typeof(System.String));
                                        columns.Add("QMSM_QSMNUM", typeof(System.String));
                                        columns.Add("QMSM_AUTKZ", typeof(System.String));
                                        columns.Add("QMSM_HANDLE", typeof(System.String));
                                        columns.Add("QMSM_TSEGFL", typeof(System.String));
                                        columns.Add("QMSM_TSEGTP", typeof(System.String));
                                        columns.Add("QMSM_TZONSO", typeof(System.String));
                                        columns.Add("QMSM_TZONSM", typeof(System.String));
                                        columns.Add("QMSM_TZONID", typeof(System.String));
                                        columns.Add("QMSM_KZACTIONBOX", typeof(System.String));
                                        columns.Add("QMSM_FUNKTION", typeof(System.String));
                                        columns.Add("QMSM_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- Savings_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("Savings.txt"))
                                    {
                                        columns.Add("SAV_SYSID", typeof(System.String));
                                        columns.Add("SAV_MANDT", typeof(System.String));
                                        columns.Add("SAV_BUKRS", typeof(System.String));
                                        columns.Add("SAV_WERKS", typeof(System.String));
                                        columns.Add("SAV_PFUID", typeof(System.String));
                                        columns.Add("SAV_CREA_DATE", typeof(System.String));
                                        columns.Add("SAV_CREA_TIME", typeof(System.String));
                                        columns.Add("SAV_PFU_CREATOR", typeof(System.String));
                                        columns.Add("SAV_UPDA_DATE", typeof(System.String));
                                        columns.Add("SAV_UPDA_TIME", typeof(System.String));
                                        columns.Add("SAV_PFU_MODIFIER", typeof(System.String));
                                        columns.Add("SAV_STATUS", typeof(System.String));
                                        columns.Add("SAV_TITLE", typeof(System.String));
                                        columns.Add("SAV_BGID", typeof(System.String));
                                        columns.Add("SAV_BGNAME", typeof(System.String));
                                        columns.Add("SAV_BUSINESS_ADMIN", typeof(System.String));
                                        columns.Add("SAV_SC1", typeof(System.String));
                                        columns.Add("SAV_SC2", typeof(System.String));
                                        columns.Add("SAV_SC3", typeof(System.String));
                                        columns.Add("SAV_SC1_DESC", typeof(System.String));
                                        columns.Add("SAV_SC2_DESC", typeof(System.String));
                                        columns.Add("SAV_SC3_DESC", typeof(System.String));
                                        columns.Add("SAV_COMM_CODE", typeof(System.String));
                                        columns.Add("SAV_SUPPLIER_ID", typeof(System.String));
                                        columns.Add("SAV_SUPPLIER_NAME", typeof(System.String));
                                        columns.Add("SAV_PROJECT_ID", typeof(System.String));
                                        columns.Add("SAV_PROJECT_NAME", typeof(System.String));
                                        columns.Add("SAV_CLASS", typeof(System.String));
                                        columns.Add("SAV_BU", typeof(System.String));
                                        columns.Add("SAV_LOB", typeof(System.String));
                                        columns.Add("SAV_YEAR", typeof(System.String));
                                        columns.Add("SAV_BUDGET_PLANNED", typeof(System.String));
                                        columns.Add("SAV_TARGET_PLANNED", typeof(System.String));
                                        columns.Add("SAV_FINAL_CONTRACT", typeof(System.String));
                                        columns.Add("SAV_SAVINGS_SECURED", typeof(System.String));
                                        columns.Add("SAV_NRC", typeof(System.String));
                                        columns.Add("SAV_NET_SAVINGS_SEC", typeof(System.String));
                                        columns.Add("SAV_NET_SAVINGS_ACT", typeof(System.String));
                                        columns.Add("SAV_COST_TO_MARKET", typeof(System.String));
                                        columns.Add("SAV_PROFIT_IMPACT", typeof(System.String));
                                        columns.Add("SAV_PROJECT_MANAGER", typeof(System.String));
                                        columns.Add("SAV_SUB_PROJECT_ID", typeof(System.String));
                                        columns.Add("SAV_SUB_PROJECT_NAME", typeof(System.String));
                                        columns.Add("SAV_REFERENCE", typeof(System.String));
                                        columns.Add("SAV_SEARCH_ITEM", typeof(System.String));
                                        columns.Add("SAV_POID", typeof(System.String));
                                        columns.Add("SAV_EXTRACT_DATE", typeof(System.String));
                                    }
                                    //------------------------------- SO_Header_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("SO_Header.txt"))
                                    {
                                        columns.Add("VBAK_SYSID", typeof(System.String));
                                        columns.Add("VBAK_MANDT", typeof(System.String));
                                        columns.Add("VBAK_VBELN", typeof(System.String));
                                        columns.Add("VBAK_ERDAT", typeof(System.String));
                                        columns.Add("VBAK_ERNAM", typeof(System.String));
                                        columns.Add("VBAK_AUDAT", typeof(System.String));
                                        columns.Add("VBAK_VBTYP", typeof(System.String));
                                        columns.Add("VBAK_TRVOG", typeof(System.String));
                                        columns.Add("VBAK_AUART", typeof(System.String));
                                        columns.Add("VBAK_AUGRU", typeof(System.String));
                                        columns.Add("VBAK_NETWR", typeof(System.String));
                                        columns.Add("VBAK_WAERK", typeof(System.String));
                                        columns.Add("VBAK_VKBUR", typeof(System.String));
                                        columns.Add("VBAK_VDATU", typeof(System.String));
                                        columns.Add("VBAK_AWAHR", typeof(System.String));
                                        columns.Add("VBAK_BSTDK", typeof(System.String));
                                        columns.Add("VBAK_KUNNR", typeof(System.String));
                                        columns.Add("VBAK_AEDAT", typeof(System.String));
                                        columns.Add("VBAK_PS_PSP_PNR", typeof(System.String));
                                        columns.Add("VBAK_VMBDAT", typeof(System.String));
                                        columns.Add("VBAK_DATUM", typeof(System.String));
                                    }
                                    //------------------------------ SO_Item_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("SO_Item.txt"))
                                    {
                                        columns.Add("VBAP_SYSID", typeof(System.String));
                                        columns.Add("VBAP_MANDT", typeof(System.String));
                                        columns.Add("VBAP_VBELN", typeof(System.String));
                                        columns.Add("VBAP_POSNR", typeof(System.String));
                                        columns.Add("VBAP_MATKL", typeof(System.String));
                                        columns.Add("VBAP_PSTYV", typeof(System.String));
                                        columns.Add("VBAP_POSAR", typeof(System.String));
                                        columns.Add("VBAP_ABGRU", typeof(System.String));
                                        columns.Add("VBAP_KLMENG", typeof(System.String));
                                        columns.Add("VBAP_MEINS", typeof(System.String));
                                        columns.Add("VBAP_NETWR", typeof(System.String));
                                        columns.Add("VBAP_WAERK", typeof(System.String));
                                        columns.Add("VBAP_WERKS", typeof(System.String));
                                        columns.Add("VBAP_AWAHR", typeof(System.String));
                                        columns.Add("VBAP_ERDAT", typeof(System.String));
                                        columns.Add("VBAP_ERNAM", typeof(System.String));
                                        columns.Add("VBAP_AEDAT", typeof(System.String));
                                        columns.Add("VBAP_PRCTR", typeof(System.String));
                                        columns.Add("VBAP_PS_PSP_PNR", typeof(System.String));
                                        columns.Add("VBAP_DATUM", typeof(System.String));

                                    }
                                    //------------------------------- Supplier_LFA1_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("Supplier_LFA1.txt"))
                                    {
                                        columns.Add("LFA1_SYSID", typeof(System.String));
                                        columns.Add("LFA1_MANDT", typeof(System.String));
                                        columns.Add("LFA1_LIFNR", typeof(System.String));
                                        columns.Add("LFA1_KRAUS", typeof(System.String));
                                        columns.Add("LFA1_NAME1", typeof(System.String));
                                        columns.Add("LFA1_NAME2", typeof(System.String));
                                        columns.Add("LFA1_NAME3", typeof(System.String));
                                        columns.Add("LFA1_NAME4", typeof(System.String));
                                        columns.Add("LFA1_STRAS", typeof(System.String));
                                        columns.Add("LFA1_PSTLZ", typeof(System.String));
                                        columns.Add("LFA1_ORT01", typeof(System.String));
                                        columns.Add("LFA1_REGIO", typeof(System.String));
                                        columns.Add("LFA1_LAND1", typeof(System.String));
                                        columns.Add("LFA1_PFACH", typeof(System.String));
                                        columns.Add("LFA1_KTOKK", typeof(System.String));
                                        columns.Add("LFA1_CONTACTFIRSTNAME", typeof(System.String));
                                        columns.Add("LFA1_CONTACTLASTNAME", typeof(System.String));
                                        columns.Add("LFA1_TELF1", typeof(System.String));
                                        columns.Add("LFA1_TELF2", typeof(System.String));
                                        columns.Add("LFA1_TELTX", typeof(System.String));
                                        columns.Add("LFA1_TELFX", typeof(System.String));
                                        columns.Add("LFA1_SPRAS", typeof(System.String));
                                        columns.Add("LFA1_ZWWLS", typeof(System.String));
                                        columns.Add("LFA1_SPERR", typeof(System.String));
                                        columns.Add("LFA1_SPERM", typeof(System.String));
                                        columns.Add("LFA1_SPERZ", typeof(System.String));
                                        columns.Add("LFA1_SPERQ", typeof(System.String));
                                        columns.Add("LFA1_NODEL", typeof(System.String));
                                        columns.Add("LFA1_ERDAT", typeof(System.String));
                                        columns.Add("LFA1_LOEVM", typeof(System.String));
                                        columns.Add("LFA1_STCD1", typeof(System.String));
                                        columns.Add("LFA1_STCD2", typeof(System.String));
                                        columns.Add("LFA1_STCEG", typeof(System.String));
                                        columns.Add("LFA1_VBUND", typeof(System.String));
                                        columns.Add("LFA1_SORTL", typeof(System.String));
                                        columns.Add("LFA1_DATUM", typeof(System.String));


                                    }
                                    //------------------------------- Supplier_LFB1_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("Supplier_LFB1.txt"))
                                    {
                                        columns.Add("LFB1_SYSID", typeof(System.String));
                                        columns.Add("LFB1_MANDT", typeof(System.String));
                                        columns.Add("LFB1_LIFNR", typeof(System.String));
                                        columns.Add("LFB1_BUKRS", typeof(System.String));
                                        columns.Add("LFB1_ZTERM", typeof(System.String));
                                        columns.Add("LFB1_ZAHLS", typeof(System.String));
                                        columns.Add("LFB1_LOEVM", typeof(System.String));
                                        columns.Add("LFB1_SPERR", typeof(System.String));
                                        columns.Add("LFB1_DATUM", typeof(System.String));
                                    }
                                    //------------------------------- Supplier_LFM1_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("Supplier_LFM1.txt"))
                                    {
                                        columns.Add("LFM1_SYSID", typeof(System.String));
                                        columns.Add("LFM1_MANDT", typeof(System.String));
                                        columns.Add("LFM1_LIFNR", typeof(System.String));
                                        columns.Add("LFM1_EKORG", typeof(System.String));
                                        columns.Add("LFM1_ZTERM", typeof(System.String));
                                        columns.Add("LFM1_LOEVM", typeof(System.String));
                                        columns.Add("LFM1_SPERM", typeof(System.String));
                                        columns.Add("LFM1_DATUM", typeof(System.String));
                                    }
                                    //-------------------------------UOM_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("UOM.txt"))
                                    {
                                        columns.Add("T006_SYSID", typeof(System.String));
                                        columns.Add("T006_MANDT", typeof(System.String));
                                        columns.Add("T006_MSEHI", typeof(System.String));
                                        columns.Add("T006_ISOCODE", typeof(System.String));
                                        columns.Add("T006_MSEHL", typeof(System.String));
                                        columns.Add("T006_DATUM", typeof(System.String));

                                    }
                                    //-------------------------------User_ETL Table -------------------------------//
                                    else if (fileUploaded.Contains("User.txt"))
                                    {
                                        columns.Add("USR03_SYSID", typeof(System.String));
                                        columns.Add("USR03_MANDT", typeof(System.String));
                                        columns.Add("USR03_BNAME", typeof(System.String));
                                        columns.Add("USR03_NAME1", typeof(System.String));
                                        columns.Add("USR03_NAME2", typeof(System.String));
                                        columns.Add("USR03_NAME_TEXT", typeof(System.String));
                                        columns.Add("USR03_ABTLG", typeof(System.String));
                                        columns.Add("USR03_DEPARTMENT", typeof(System.String));
                                        columns.Add("USR03_DATUM", typeof(System.String));
                                    }

                                    else
                                {

                                    Console.WriteLine("--------------------------------------------------------------------------------------------------------------------------------------");
                                    Console.WriteLine("-- NO TBLS found --");
                                    Console.WriteLine("--------------------------------------------------------------------------------------------------------------------------------------");
                                    Console.WriteLine("");
                                    Console.WriteLine("");

                                }


                                ////////////////////////////////////////////////////end of columns definition//////////////////////////////////////////////////////////////////////////////////////////////////////

                                Console.WriteLine("UPLOADING OF: " + fileUploaded);
                                Console.WriteLine("IN TBL: " + TableName.ToString());


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
                                        rows = rows + 1;
                                        if (batchsize == 100000)
                                        {

                                            bulkcopy.WriteToServer(datatable);
                                            datatable.Rows.Clear();
                                            batchsize = 0;
                                            Console.WriteLine("Total Flushed rows for " + rows.ToString() + " -> " + TableName + ": " + totalFlushedLines.ToString());
                                        }

                                    }
                                    catch (ArgumentException ex)
                                    {
                                        //Console.Error.Write("Input: \r\n");

                                        Console.Error.WriteLine("Input error: the exception message: {0}", ex.Message);
                                        //string[] line = reader.ReadLine().Split('|');
                                        //addded the problematic line error
                                        rows = rows + 1;
                                        errors.Add("Error found in line: " + (rows).ToString());
                                        //Counter of errors
                                        error = error + 1;
                                    }

                                }

                                try
                                {
                                    bulkcopy.WriteToServer(datatable);
                                    datatable.Rows.Clear();
                                }
                                catch (ArgumentException ex)
                                {
                                    Console.Error.WriteLine("Input error: the exception message: {0}", ex.Message);
                                }

                            }
                        }
                    }
                    elapsed.Stop();
                    using (System.IO.TextWriter tw = new System.IO.StreamWriter(Path))
                    {
                        foreach (string line in errors)
                        {
                            tw.WriteLine(string.Format("Line Error: n." + line.ToString()));
                        }
                    }
                    Console.WriteLine("--------------------------------------------------------------------------------------------------------------------------------------");
                    Console.WriteLine((rows - error + " records imported in " + elapsed.Elapsed.TotalSeconds + " seconds. Number of lines skipped because of error: " + error));
                    Console.WriteLine("Table: " + TableName);
                    Console.WriteLine("Erros saved in file: \r\n" + Path);
                    Console.WriteLine("-------------------------------------------------------------------------------------------------------------------------------------");
                    Console.WriteLine("");
                    Console.WriteLine("");
                    rows = 0;
                     //saving the list of error s in file under Path
                    }

                }

            }

            ///////////////////////////////////////////////////////////////////////
            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("-------------------------------------------------------------------------------------------------------------------------------------");
            Console.WriteLine("Operation completed. Please push any button to exit Console. Ciao CIao by SanGy!!! :D");
            Console.WriteLine("-------------------------------------------------------------------------------------------------------------------------------------");
            Console.WriteLine("");
            Console.WriteLine("");
            Console.ReadLine();
        }
    }
}
