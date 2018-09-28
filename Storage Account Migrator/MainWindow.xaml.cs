using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace Storage_Account_Migrator
    {
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
        {
        private Delegates.UpdateLogConsole m_updateConsoleDelegate;
        public MainWindow ()
            {
            InitializeComponent ();
            m_updateConsoleDelegate = new Delegates.UpdateLogConsole (ReportSeqQueryProgress);
            m_updateConsoleDelegate ("Ready to migirate. click start . . . . ");
            }

        private void Button_Click (object sender, RoutedEventArgs e)
            {
            if (string.IsNullOrWhiteSpace (txtSource.Text))
                {
                MessageBox.Show ("Source Connection string required");
                return;
                }
            if (string.IsNullOrWhiteSpace (txtTarget.Text))
                {
                MessageBox.Show ("Source Connection string required");
                return;
                }

            this.StartTheThread ();
            }
        private void ReportSeqQueryProgress
            (
            string message,
            bool isError = false
            )
            {
            Object obj = new Object ();
            lock (obj)
                {
                string msg = $"{DateTime.Now} : {message}";

                txtConsole.Dispatcher.BeginInvoke (new Action (() =>
                {
                    if (isError)
                        txtConsole.Foreground = new SolidColorBrush (Colors.Red);
                    else
                        txtConsole.Foreground = new SolidColorBrush (Colors.Green);

                    txtConsole.Text += msg + Environment.NewLine;
                }));
                }
            }
        public void StartTheThread ()
            {
            StorageTableMigrator storageAccountMigrator = new StorageTableMigrator (txtSource.Text, txtTarget.Text, m_updateConsoleDelegate);
            m_updateConsoleDelegate ("StorageMigrator instance created...");
            var t = new Thread (() => storageAccountMigrator.Start ().Wait ());
            t.Start ();;
            }
        }
    }
