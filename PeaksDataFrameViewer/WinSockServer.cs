using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace youFast
{
    public class WinSockServer
    {
        TcpListener mTCPListener;
        List<TcpClient> mClients = new List<TcpClient>();

        public async void StartListeningForIncomingConnection(IPAddress ipaddr, int port, byte[] file)
        {
            mTCPListener = new TcpListener(IPAddress.Any, port);
            try
            {
                mTCPListener.Start();

                while (true)
                {
                    TcpClient paramClient = await mTCPListener.AcceptTcpClientAsync();

                    mClients.Add(paramClient);

                    Console.WriteLine(
                        string.Format("Client connected successfully, count: {0} - {1}",
                        mClients.Count, paramClient.Client.RemoteEndPoint)
                        );
                    TakeCareOfTCPClient(paramClient, file);
                }
            }
            catch (Exception excp)
            {
                Console.WriteLine(excp.ToString());
            }
        }

        public void StopServer()
        {
            try
            {
                if (mTCPListener != null)
                {
                    mTCPListener.Stop();
                }

                foreach (TcpClient c in mClients)
                {
                    c.Close();
                }

                mClients.Clear();
            }
            catch (Exception excp)
            {

                Console.WriteLine(excp.ToString());
            }
        }

        private async void TakeCareOfTCPClient(TcpClient paramClient, byte[] file)
        {
            NetworkStream stream = null;
            StreamReader reader = null;
            int nRet;

            try
            {
                stream = paramClient.GetStream();
                reader = new StreamReader(stream);

                char[] buff = new char[128];               
                var startReceive = DateTime.Now;
                StringBuilder csvString = new StringBuilder();
                while (true)
                {
                    nRet = await reader.ReadAsync(buff, 0, buff.Length);
                    Console.WriteLine("nRet = " + nRet + "  " + paramClient.Client.RemoteEndPoint);
                    if (nRet == 0)
                    {
                        RemoveClient(paramClient);
                        Console.WriteLine("Socket disconnected");
                        break;
                    }
                    byte[] buffMessage = Encoding.ASCII.GetBytes(buff);
                    string receivedText = new string(buff);
                    Console.WriteLine(receivedText.Trim());
                    var x = string.Compare(receivedText.Trim(), "downloadDB");
                    Console.WriteLine("aaaaa" + x);

                    if (x == 0)
                    {
                        paramClient.GetStream().WriteAsync(file, 0, file.Length);
                    }
                    Array.Clear(buff, 0, buff.Length);
                }
            }
            catch (Exception excp)
            {
                RemoveClient(paramClient);
                Console.WriteLine("Client Disconnected Exception");
            }

        }

        private void RemoveClient(TcpClient paramClient)
        {
            if (mClients.Contains(paramClient))
            {
                mClients.Remove(paramClient);
                Console.WriteLine(String.Format("Client removed, count: {0}", mClients.Count));
            }
        }
    }
}
