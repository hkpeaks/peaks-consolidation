using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;

namespace youFast
{
    public class WinSockClient
    {
        public void winSock(int iteration, string outputFolder, Dictionary<int, string> forwardMessage, char csvWriteSeparator, Dictionary<string, string> columnName2ID, Dictionary<int, StringBuilder> htmlTable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, List<double>> ramDetailgz, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<int, Dictionary<string, double>> ramValue2Keygz, Dictionary<int, Dictionary<double, double>> ramKey2Order, Dictionary<int, Dictionary<double, double>> ramOrder2Key)
        {
            Socket client = null;
            client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            int n = 0;

            try
            {
                client.Connect("192.168.1.132", 7000);

                string inputCommand = string.Empty;

                while (true)
                {
                    Thread.Sleep(200);                         

                    if (forwardMessage[0] == "downloadDB")
                    {

                        byte[] buffSend = Encoding.ASCII.GetBytes(forwardMessage[0]);
                        client.Send(buffSend);
                        int nRecv;
                        int total = 0;
                        byte[] buffReceived = new byte[128];
                        byte[] fullFile = new byte[11204185];
                        var startReceiveFileTime1 = DateTime.Now;
                        if (forwardMessage[0] == "downloadDB") forwardMessage[0] = "waiting";

                        using (MemoryStream ms = new MemoryStream())
                        {
                            do
                            {
                                nRecv = client.Receive(buffReceived);
                                ms.Write(buffReceived, 0, nRecv);
                                total = total + nRecv;
                            } while (total < 11204185);
                            var endReceiveFileTime1 = DateTime.Now;
                            Console.WriteLine("");
                            Console.WriteLine("");
                            Console.WriteLine("Download Database to In-Ramory of Local Machine  Time:     " + String.Format("{0:F3}", (endReceiveFileTime1 - startReceiveFileTime1).TotalSeconds) + " seconds");

                            var startReceiveFileTime2 = DateTime.Now;

                            using (FileStream fs = new FileStream(@"ramDetail" + n + ".db", FileMode.OpenOrCreate))
                            {
                                ms.WriteTo(fs);
                                fs.Flush();
                            }

                            var endReceiveFileTime2 = DateTime.Now;

                            Console.WriteLine("Save In-Ramory Database to Disk of Local Machine Time:     " + String.Format("{0:F3}", (endReceiveFileTime2 - startReceiveFileTime2).TotalSeconds) + " seconds");

                            var startReceiveFileTime3 = DateTime.Now;
                            BinaryFormatter formatter1 = new BinaryFormatter();

                            using (GZipStream gZipStream = new GZipStream(File.OpenRead(@"ramDetail" + n + ".db"), CompressionMode.Decompress))
                            {
                                ramDetailgz = (Dictionary<int, List<double>>)formatter1.Deserialize(gZipStream);
                            }
                            var endReceiveFileTime3 = DateTime.Now;
                            Console.WriteLine("Number of Column x Row:  " + ramDetailgz[0].Count + " x " + ramDetailgz[0].Count + " = " + ramDetailgz[0].Count * ramDetailgz[0].Count);

                            Console.WriteLine("Restore Disk to In-ramory of Local Machine Time:     " + String.Format("{0:F3}", (endReceiveFileTime3 - startReceiveFileTime3).TotalSeconds) + " seconds");

                            Dictionary<int, List<double>> ramDetail = new Dictionary<int, List<double>>();   
                        }
                    }
                }
            }
            catch (Exception excp)
            {
                Console.WriteLine(excp.ToString());
            }
            finally
            {
                if (client != null)
                {
                    if (client.Connected)
                    {
                        client.Shutdown(SocketShutdown.Both);
                    }
                    client.Close();
                    client.Dispose();
                }
            }
           
            Console.WriteLine("Press a key to exit...");
            Console.ReadKey();
        }
    }
}
