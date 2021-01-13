// In His Name

package mymapreduce;

import java.util.Scanner;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.*;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Comparator;

public class MapReduce{

    private Path userPath;

    public Stream<Pair<String, List<String>>> data;

    public MapReduce() throws IOException { // this invokes whenever we make a new instance
        final Path userPath = getpath();
        this.userPath = userPath;
        this.data = read();
    }

    public static Path getpath() {                                                                               //geting the rootPath from User 
        Scanner myObj = new Scanner(System.in);
        System.out.println("Please Enter the absolute path of the directory where documents are stored: ");
        String userPathS = myObj.nextLine();
        Path userPath = Paths.get(userPathS);                  //Converting String to Path
        myObj.close();
        return userPath;
    }

    public Stream<Pair<String, List<String>>> read() throws IOException {                        //return a stream of pairs (fileName, contents)
        List<Path> paths = Files.walk(userPath).collect(Collectors.toList());
        return paths.stream().filter(Files::isRegularFile)                                       // Only regular files
                .filter(path -> path.getFileName().toString().endsWith(".txt"))                  //Only consider a file if it's a text file
                .map(path -> new Pair<String, List<String>>(path.getFileName().toString(), myReadAllLines(path))); // for each file return pair of name, read all lines
    }
    
    private static List<String> myReadAllLines(Path path) {
        try {
            return Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Stream<Pair<String, Integer>> map(Stream<Pair<String, List<String>>> data){

        List<Pair<String, Integer>> result = new ArrayList<Pair<String, Integer>>();
        data.map(BL -> BL.getValue())                                                    // Now we have the list of Lines
                .map(lines -> lines.stream().map(line -> countline(line)))               // Now It counts words of a line
                .forEach(contentCount -> contentCount.forEach(Linecount -> Linecount.forEach(WordCount -> result.add(WordCount))));       // added Pair to Result
        return result.stream();            //Result was List, Return as Stream
        
    }
    
    public static Stream<Pair<String, Integer>> countline(String line) {               //count words of a line
        List<String> words = Arrays.asList(line.split(" "));
        return words
        .stream()
        .filter(wrod -> wrod.length() >= 3)
        .map(word -> count(word, line))
        .map(i ->new Pair<String, Integer>(i.getKey(), i.getValue()));

    }
    
    public static Pair<String, Integer> count(String word, String line) {           // count number of occurrences of a word in line
        int c = 0;
        Pattern p = Pattern.compile(word);
        Matcher m = p.matcher(line);
        while (m.find()) {
            c++;
        }
        return new Pair<String, Integer>(word, c);
    }
    
    public static Integer compare(String a , String b){                                                            //compare Strings according to alphanumeric order
        a = a.chars().sorted().mapToObj(c -> (char) c)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();    // change a to alphanumeric order
        b = b.chars().sorted().mapToObj(c -> (char) c)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();    // change b to alphanumeric order
        return a.compareTo(b);                                                                                     // comparing which adher to java conventions
    }
    
    public static Stream<Pair<String, Integer>> reduce(Stream<Pair<String, List<Integer>>> data) {

        return data.map(i -> new Pair<String, Integer>(i.getKey(), i.getValue().stream().reduce(0, Integer::sum)));  //reduce List of integer to sum
    }

    public static void write(File dst, Stream<Pair<String, Integer>> res) throws FileNotFoundException {              
        PrintStream ps = new PrintStream(dst);
        res.sorted(Comparator.comparing(Pair::getKey)).forEach(p -> ps.println(p.getKey() + ", "+  p.getValue()));
        ps.close();
    }
    



}