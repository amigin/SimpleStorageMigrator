using System;
using System.Collections.Generic;

namespace TableStorageMigrator
{
    class Program
    {
        static void Main(string[] args)
        {
            var srcConnString = Environment.GetEnvironmentVariable("SrcConnString");
            if (string.IsNullOrEmpty(srcConnString))
                throw new Exception("Please specift 'SrcConnString' env variable");

            var destConnString = Environment.GetEnvironmentVariable("DestConnString");
            if (string.IsNullOrEmpty(destConnString))
                throw new Exception("Please specift 'DestConnString' env variable");


            var verifyTables = SettingsReader.VerifyTables();


            SimpleCopyPasteEngine.CopyPaste(srcConnString, destConnString, verifyTables);

            Console.WriteLine("Done....");
            Console.ReadLine();

        }


    }



}