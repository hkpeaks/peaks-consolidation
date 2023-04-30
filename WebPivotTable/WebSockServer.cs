using Fleck;
using System;
using System.Collections.Generic;

namespace youFast
{
    public class WebSockServer
    {
        public void webSock()
        {

            var clients = new List<IWebSocketConnection>();

            var server = new WebSocketServer("ws://192.168.1.132:9000");

            server.Start(socket =>
            {
                socket.OnOpen = () =>
                {
                    clients.Add(socket);
                };

                socket.OnClose = () =>
                {
                    clients.Remove(socket);
                };

                socket.OnMessage = message =>
                {
                    socket.Send(message);
                    if (message == "a") Console.WriteLine("sdffsdfsdgfdgdffgdgfd");
                };

                socket.OnBinary = message =>
                {

                };
            });

        }

    }
}
