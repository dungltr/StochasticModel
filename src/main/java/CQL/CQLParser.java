package CQL;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.z3950.zing.cql.CQLNode;
import org.z3950.zing.cql.CQLParseException;
import org.z3950.zing.cql.CQLParser;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static java.lang.System.out;
import static org.junit.Assert.assertEquals;

class CQLParserTest {
    public CQLParserTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }
    public static void main(String[] args) throws IOException {
        //test();
    }
    public static String[] listFolder(String path){
        File[] directories = new File(path).listFiles(File::isDirectory);
        String [] nameFolders = new String[directories.length];
        for (int i = 0; i< directories.length;i++){
            nameFolders[i] = directories[i].toString();
            nameFolders[i] = nameFolders[i].replace(path,"").replace("/","");

        }
        return nameFolders;
    }
    public static String[] listFile(String path){
        File[] directories = new File(path).listFiles(File::isFile);
        String [] nameFolders = new String[directories.length];
        for (int i = 0; i< directories.length;i++){
            nameFolders[i] = directories[i].toString();
            nameFolders[i] = nameFolders[i].replace(path,"").replace("/","");

        }
        return nameFolders;
    }
    public static void test() throws IOException {
        System.out.println("Testing the parser using pre-canned regression queries...");
        //we might be running the test from within the jar
        //list all resource dirs, then traverse them
        String directories = "/Users/letrung/Downloads/cql-java-master/src/test/resources/regression";
        String[] dirs = listFolder(directories);
        //String[] dirs = getResourceListing("/Users/letrung/Downloads/cql-java-master/src/test/resources", "regression");
        for (String dir : dirs) {
            //System.out.println("Parsing "+dir);
            //String files[] = getResourceListing(this.getClass(), "regression/" + dir);
            String files[] = listFile(directories + "/" + dir);
            for (String file : files) {
                //System.out.println("Parsing "+dir+"/"+file);
                if (!file.endsWith(".cql")) continue;
                System.out.println("Parsing "+dir+"/"+file);
                //InputStream is = this.getClass().getResourceAsStream("/regression/"+dir+"/"+file);
                InputStream is = new FileInputStream(directories +"/"+dir+"/"+file);
                //InputStream is = IOUtils.toInputStream(directories +"/"+dir+"/"+file);
                //is.// (directories+"/"+dir+"/"+file);
                BufferedReader reader = null, reader2 = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(is));
                    String input = reader.readLine();
                    input = "select * from orders, lineitem where l_orderkey == o_orderkey;";
                    out.println("Query: "+input);
                    String result;
                    try {
                        CQLParser parser = new CQLParser();
                        CQLNode parsed = parser.parse(input);
                        result = parsed.toXCQL();
                    } catch (CQLParseException pe) {
                        result = pe.getMessage() + "\n";
                    }
                    out.println("Parsed:");
                    out.println(result);
                    //read the expected xcql output
                    String expected = "<expected result file not found>";
                    String prefix = file.substring(0, file.length()-4);
                    //InputStream is2 = this.getClass()
                    //        .getResourceAsStream("/regression/"+dir+"/"+prefix+".xcql");
                    InputStream is2 = new FileInputStream(directories +"/"+dir+"/"+prefix+".xcql");
                    if (is2 != null) {
                        reader2 = new BufferedReader(new InputStreamReader(is2));
                        StringBuilder sb = new StringBuilder();
                        String line;
                        while ((line = reader2.readLine()) != null) {
                            sb.append(line).append("\n");
                        }
                        expected = sb.toString();
                    }
                    out.println("Expected: ");
                    out.println(expected);
                    //assertEquals("Assertion failure for "+dir+"/"+file, expected, result);
                } finally {
                    if (reader != null) reader.close();
                    if (reader2 != null) reader2.close();
                }
            }
        }
    }
    @SuppressWarnings("rawtypes")
    public static String[] getResourceListing(Class clazz, String path) throws
            IOException {
        URL dirURL = clazz.getClassLoader().getResource(path);
        if (dirURL != null && dirURL.getProtocol().equals("file")) {
      /* A file path: easy enough */
            try {
                return new File(dirURL.toURI()).list();
            } catch (URISyntaxException use) {
                throw new UnsupportedOperationException(use);
            }
        }

        if (dirURL == null) {
      /*
       * In case of a jar file, we can't actually find a directory.
       * Have to assume the same jar as clazz.
       */
            String me = clazz.getName().replace(".", "/") + ".class";
            dirURL = clazz.getClassLoader().getResource(me);
        }

        if (dirURL.getProtocol().equals("jar")) {
      /* A JAR path */
            String jarPath = dirURL.getPath().substring(5, dirURL.getPath().indexOf(
                    "!")); //strip out only the JAR file
            JarFile jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"));
            Enumeration<JarEntry> entries = jar.entries(); //gives ALL entries in jar
            Set<String> result = new HashSet<String>(); //avoid duplicates in case it is a subdirectory
            while (entries.hasMoreElements()) {
                String name = entries.nextElement().getName();
                if (name.startsWith(path)) { //filter according to the path
                    String entry = name.substring(path.length());
                    int checkSubdir = entry.indexOf("/");
                    if (checkSubdir >= 0) {
                        // if it is a subdirectory, we just return the directory name
                        entry = entry.substring(0, checkSubdir);
                    }
                    result.add(entry);
                }
            }
            return result.toArray(new String[result.size()]);
        }

        throw new UnsupportedOperationException("Cannot list files for URL "
                + dirURL);
    }
}
