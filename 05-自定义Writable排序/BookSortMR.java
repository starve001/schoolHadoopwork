package hadoop01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class Book implements WritableComparable<Book> {
    private String cardNo;
    private String indexNo;
    private String bookName;
    private String borrowDate;
    private String returnDate;
    private String status;
    private String category;
    private String language;
    private String location;
    private String callNo;
    private String author;

    public Book() {}

    public Book(String cardNo, String indexNo, String bookName,
                String borrowDate, String returnDate, String status,
                String category, String language, String location,
                String callNo, String author) {
        this.cardNo = cardNo;
        this.indexNo = indexNo;
        this.bookName = bookName;
        this.borrowDate = borrowDate;
        this.returnDate = returnDate;
        this.status = status;
        this.category = category;
        this.language = language;
        this.location = location;
        this.callNo = callNo;
        this.author = author;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(cardNo);
        out.writeUTF(indexNo);
        out.writeUTF(bookName);
        out.writeUTF(borrowDate);
        out.writeUTF(returnDate);
        out.writeUTF(status);
        out.writeUTF(category);
        out.writeUTF(language);
        out.writeUTF(location);
        out.writeUTF(callNo);
        out.writeUTF(author);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.cardNo = in.readUTF();
        this.indexNo = in.readUTF();
        this.bookName = in.readUTF();
        this.borrowDate = in.readUTF();
        this.returnDate = in.readUTF();
        this.status = in.readUTF();
        this.category = in.readUTF();
        this.language = in.readUTF();
        this.location = in.readUTF();
        this.callNo = in.readUTF();
        this.author = in.readUTF();
    }

    @Override
    public int compareTo(Book o) {
        int cmp = this.indexNo.compareTo(o.indexNo);
        if (cmp != 0) return cmp;
        return this.cardNo.compareTo(o.cardNo);
    }

    @Override
    public String toString() {
        return cardNo + "\t" + indexNo + "\t" + bookName + "\t"
                + borrowDate + "\t" + returnDate + "\t" + status + "\t"
                + category + "\t" + language + "\t" + location + "\t"
                + callNo + "\t" + author;
    }

    public String getCardNo() { return cardNo; }
    public void setCardNo(String cardNo) { this.cardNo = cardNo; }
    public String getIndexNo() { return indexNo; }
    public void setIndexNo(String indexNo) { this.indexNo = indexNo; }
    public String getBookName() { return bookName; }
    public void setBookName(String bookName) { this.bookName = bookName; }
    public String getBorrowDate() { return borrowDate; }
    public void setBorrowDate(String borrowDate) { this.borrowDate = borrowDate; }
    public String getReturnDate() { return returnDate; }
    public void setReturnDate(String returnDate) { this.returnDate = returnDate; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public String getLanguage() { return language; }
    public void setLanguage(String language) { this.language = language; }
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    public String getCallNo() { return callNo; }
    public void setCallNo(String callNo) { this.callNo = callNo; }
    public String getAuthor() { return author; }
    public void setAuthor(String author) { this.author = author; }
}

// 文件名为 BookSortMR.java，公共类名同步改为 BookSortMR
public class BookSortMR {
    static class BookMapper extends Mapper<LongWritable, Text, Book, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] fields = line.split("\t");
            if (fields.length < 11) return;
            Book book = new Book(
                    fields[0], fields[1], fields[2], fields[3], fields[4],
                    fields[5], fields[6], fields[7], fields[8], fields[9], fields[10]
            );
            context.write(book, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "books_secondary_sort");
        job.setJarByClass(BookSortMR.class);
        job.setMapperClass(BookMapper.class);
        job.setMapOutputKeyClass(Book.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Book.class);
        job.setOutputValueClass(NullWritable.class);

        Path outputPath = new Path("/library/sort_output");
        if (fs.exists(outputPath)) fs.delete(outputPath, true);
        FileInputFormat.addInputPath(job, new Path("/library/input/books.txt"));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}