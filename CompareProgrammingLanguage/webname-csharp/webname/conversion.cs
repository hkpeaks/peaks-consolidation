namespace WebName {

    public class Conversion {
        public byte[] file2bytestream(string filename) {
            byte[] bytestream = File.ReadAllBytes(filename);
            return bytestream;
        }
    }
}
       
     






