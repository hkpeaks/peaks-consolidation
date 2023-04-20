namespace WebName
{
    class Program
    {
        static void Main(string[] args)
        {  
             ProcessFlow  current_process = new ProcessFlow();
             current_process.webname(args[0].ToString());             
        }

    }

}