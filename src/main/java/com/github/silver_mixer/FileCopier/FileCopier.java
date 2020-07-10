package com.github.silver_mixer.FileCopier;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

public class FileCopier{
	private static Calendar cal = Calendar.getInstance();
	private static final String[] HELP_MESSAGE = {
			"File Copier v0.0.1",
			"使用法: java -jar FileCopier.jar [Option...] <SOURCE DIRECTORY> <TARGET DIRECTORY>",
			"",
			"オプション:",
			"  --help        ヘルプメッセージを表示",
			"  -d <name>     データベースにコピー情報を保存",
			"  -e <number>   処理を中止する連続エラー回数(初期値:0(=なし))"};
	private static Path source, target;
	private static Statement state;
	private static PreparedStatement pstate;
	private static Set<String> fileHashes = new HashSet<String>();
	private static int failStopCount = -1, curFailCount = 0;
	
	public static void main(String[] args) {
		String dbname = null, sourcePath = null, targetPath = null;
		for(int i = 0; i < args.length; i++) {
			if(args[i].equals("--help")) {
				printHelpMessage();
				System.exit(0);
			}else if(args[i].equals("-d")) {
				if(i + 1 == args.length) {
					printHelpMessage();
					System.exit(1);
				}
				dbname = args[++i];
			}else if(args[i].equals("-e")) {
				if(i + 1 == args.length) {
					printHelpMessage();
					System.exit(1);
				}
				try {
					failStopCount = Integer.parseInt(args[++i]);
				}catch(NumberFormatException e) {
					System.out.println("[エラー] 回数は数値で指定して下さい。");
					System.exit(1);
				}
			}else if(i == args.length - 2) {
				sourcePath = args[i];
			}else if(i == args.length - 1) {
				targetPath = args[i];
			}
		}
		if(sourcePath == null || targetPath == null) {
			printHelpMessage();
			System.exit(1);
		}
		if(dbname != null) {
			try {
				File dbfile = new File(Paths.get(FileCopier.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent().toString() + "/db/", dbname + ".db");
				dbfile.getParentFile().mkdirs();
				System.out.println("dbfile: " + dbfile.getAbsolutePath());
				Connection con = DriverManager.getConnection("jdbc:sqlite:" + dbfile.getAbsolutePath());
				state = con.createStatement();
				state.executeUpdate("CREATE TABLE IF NOT EXISTS transferfiles (hash TEXT NOT NULL PRIMARY KEY, source TEXT, destination TEXT)");
				pstate = con.prepareStatement("INSERT INTO transferfiles VALUES (?, ?, ?)");
			}catch(URISyntaxException | SQLException e) {
				e.printStackTrace();
				System.out.println("[エラー] データベースの生成に失敗。");
				System.exit(1);
			}
			System.out.println("[情報] DATABASE: " + dbname);
		}
		source = Paths.get(sourcePath);
		target = Paths.get(targetPath);
		System.out.println("[情報] SOURCE: " + sourcePath);
		System.out.println("[情報] TARGET: " + targetPath);
		if(!target.toFile().exists()) {
			try {
				Files.createDirectories(target);
			}catch(IOException e) {
				e.printStackTrace();
				System.out.println("[エラー] TARGET DIRECTORYの生成に失敗。");
				System.exit(1);
			}
		}
		copyAllFilesRecursively(source);
		try {
			pstate.executeBatch();
			System.out.println("[情報] データベースを更新しました。");
		}catch(SQLException e) {
			e.printStackTrace();
			System.out.println("[エラー] データベースの保存に失敗。");
			System.exit(1);
		}
		System.out.println("[情報] 操作が完了しました。");
	}
	
	@SuppressWarnings("unused")
	public static void copyAllFilesRecursively(Path parent) {
		if(!parent.toFile().isDirectory())parent = parent.toFile().getParentFile().toPath();
		File[] files = parent.toFile().listFiles();
		for(File f: files) {
			if(f.isDirectory()) {
				copyAllFilesRecursively(f.toPath());
			}else {
				String md5 = "";
				try(FileInputStream fis = new FileInputStream(f)) {
					md5 = DigestUtils.md5Hex(fis);
				}catch(IOException e) {
					e.printStackTrace();
					System.out.println("[エラー] ハッシュ値確認中にエラーが発生しました。");
					continue;
				}
				try {
					if(fileHashes.contains(md5) || state.executeQuery("SELECT * FROM transferfiles WHERE hash = '" + md5 + "'").next()) {
						System.out.println("[変更なし] " + source.relativize(f.toPath()).toString());
					}else {
						cal.setTimeInMillis(f.lastModified());
						String year = String.format("%04d", cal.get(Calendar.YEAR)),
								month = String.format("%02d", cal.get(Calendar.MONTH) + 1),
								date = String.format("%02d", cal.get(Calendar.DATE)),
								hour24 = String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)),
								hour12 = String.format("%02d", cal.get(Calendar.HOUR)),
								minute = String.format("%02d", cal.get(Calendar.MINUTE)),
								second = String.format("%02d", cal.get(Calendar.SECOND)),
								millisecond = String.format("%03d", cal.get(Calendar.MILLISECOND));
						String fileName = year + month + date + "_" + hour24 + minute + second;
						String fileExt = (f.getName().contains(".") ? f.getName().substring(f.getName().lastIndexOf(".")) : "");
						int conflict = 0;
						Path newFilePath = Paths.get(target.toString(), fileName + "_" + String.format("%03d", conflict)+ fileExt);
						while(newFilePath.toFile().exists()) {
							conflict++;
							newFilePath = Paths.get(target.toString(), fileName + "_" + String.format("%03d", conflict)+ fileExt);
						}
						Files.copy(f.toPath(), newFilePath, StandardCopyOption.REPLACE_EXISTING);
						newFilePath.toFile().setLastModified(f.lastModified());
						curFailCount = 0;
						fileHashes.add(md5);
						pstate.setString(1, md5);
						pstate.setString(2, f.getAbsolutePath());
						pstate.setString(3, target.toAbsolutePath().toString() + "/" + f.getName());
						pstate.addBatch();
						System.out.println("[コピー] " + source.relativize(f.toPath()).toString() + " => " + newFilePath.toAbsolutePath().toString());
					}
				}catch(SQLException e) {
					e.printStackTrace();
					System.out.println("[エラー] SQLの操作中にエラーが発生しました。");
				}catch(IOException e) {
					e.printStackTrace();
					System.out.println("[エラー] ファイルのコピー中にエラーが発生しました。");
					curFailCount++;
					if(curFailCount == failStopCount) {
						System.out.println("[情報] エラーが連続で" + failStopCount + "回発生したため、処理を中止します。");
					}
				}
			}
		}
	}
	
	public static void printHelpMessage() {
		for(String s: HELP_MESSAGE)System.out.println(s);
	}
}
