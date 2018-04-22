using System;
using System.Collections.Generic;

namespace TableStorageMigrator
{
    class Program
    {
        static void Main(string[] args)
        {

            var settings = SettingsReader.GetSettings();

            if (settings.Mode == SettingsModel.UploadNonExistMode)
                settings.UploadMissingRecords();
            else
                settings.RunSimpleCopyPaste();

            Console.WriteLine("Done....");
            Console.ReadLine();

        }

    }



}