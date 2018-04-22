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
            if (settings.Mode == SettingsModel.SimpleCopyMode)            
                settings.RunSimpleCopyPaste();
            else
            {
                Console.WriteLine("Unknown mode: '"+settings.Mode+"'");
            }

            Console.WriteLine("Done....");
            Console.ReadLine();

        }

    }



}