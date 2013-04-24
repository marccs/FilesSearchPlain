
import scala.actors._
import scala.actors.Actor
import Actor._
import java.io.File
import java.io.FileFilter
import java.io.FilenameFilter
import scala.collection.mutable



case object Stop
case object Crawl
case object Index
case class Index(f : java.io.File)
case class msg(f : java.io.File)

case class startLog(filename : String)
case class msgLog(str : String)
case class StopLog

case class Word(w : String, path_filename : String)
case class NewIndexer(in : Indexer)
case class StartFileScanner

case class Search
case class SearchIndexer(str : String)

case object check
case class Paths(str : String)




object logFileActor extends Actor {

	private var logFileNameVar : String = null;

	def logFileName(f : String) = {
		logFileNameVar = f;
	}

	def getCurrentDateTime : String = {
		var cal : java.util.Calendar = new java.util.GregorianCalendar
		var creationDate : java.util.Date = cal.getTime();
		var date_format : java.text.SimpleDateFormat  = new java.text.SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
		return date_format.format(creationDate)
	}

	def act() {

		// List to save words
		var words = scala.collection.mutable.Buffer[String]()
	
		var conta : Int = 0
		var output : java.io.FileWriter = new java.io.FileWriter(logFileNameVar)
	
		println("[logFileActor] starting... ");
	
		loop {
			react {
				case msgLog(s : String) => {
					try {
						var writer  : java.io.BufferedWriter = new java.io.BufferedWriter(output,32 * 1024);
						writer.write(s+"\n");
						writer.flush();
					} catch {
						case e:java.io.IOException => {
							e.printStackTrace();
						}
					}
				}
				case StopLog => {
					output.close();
					println("[logFileActor] stopping... ");
					exit();
				}
			}
		}

	}

}



class FileCrawler(fileScanner : Actor, fileFilter : FileFilter, root : File) extends Actor {


	def act() {

		println("[FileCrawler] starting...");

		loop {
			react {
				case Crawl => {

					try {
						crawl(root);
					} catch {
						case e:InterruptedException =>
							println("[FileCrawler] Got local exception: " + e);
							System.exit(0);
						case e:Exception =>
							println("other EXCEPTION"+e.printStackTrace());
					}
				
					this ! Stop
				}
				case Stop =>
					fileScanner ! Stop
					println("[FileCrawler] stopping...");
					exit();
			}
		}

	}


	def alreadyIndexed(f : File) : Boolean = return false


	@throws(classOf[java.lang.InterruptedException])
	def crawl(root : File) {

		var entries : Array[File] = null

		entries = root.listFiles(fileFilter)

		if (entries != null) {
			entries.foreach { entry =>
				if (!entry.isHidden()) {
					if (entry.isDirectory()) {
						fileScanner ! Index(entry)
						crawl(entry)
					}
					else if (!alreadyIndexed(entry)) {
						fileScanner ! Index(entry)
					}
				}
			}
		}
	}

}


class FileScanner(mainActor : Actor) extends Actor {

	var indx : Indexer = null

	def act() {

		println("[FileScanner] starting...")

		var resultIndexFile : Boolean = true

		loop {
			react {
				case StartFileScanner => {
					indx =  new Indexer(this)
					Main.indexers = indx :: Main.indexers
					indx.start
				}
				case Index(fi : java.io.File) => {
					indexFile(fi)
				}
				case NewIndexer(in : Indexer) => {
					indx = in
				}
				case Stop => {
					println("[FileScanner] stopping...")
					mainActor ! Search
					exit()
				}
			}
		}
	}


	def indexFile(file : java.io.File) = {

		// Index the file...
		var fileName : String = file.getName()
		var absolutePath : String = file.getAbsolutePath()
		var filePath : String = absolutePath.substring(0,absolutePath.lastIndexOf(File.separator))
		
		var wordArray : Array[String] = splitFileName(fileName)

		wordArray.foreach { w =>
			if (w != "") {
				w.trim()
				indx ! Word(w, filePath+"/"+fileName)
			}
		}
	
	}

	def splitFileName(fileName : String) : Array[String] = {
		val fileNameArray : Array[String] = fileName.split("[-:. _]+");
		return fileNameArray;
	}

}

// Indexer
class Indexer(fsScanner : FileScanner) extends Actor {

	// ConcurrentHaspMap allows atomic operations
	// hash, index
	val wordsHashMap = new java.util.Hashtable[Long, Int](5000)

	// Paths for the word
	var arrayOfPaths : Array[Array[String]] = new Array[Array[String]](5000)
	var Index : Int = 0

	def  insertArrayOfPaths(w : String, path_filename : String) {

		var hashkey 		: Long 		= 0L
		var index_temp 		: Int 		= 0
		var j 				: Int 		= 0
		var existeHashkey 	: Boolean 	= false

		hashkey = ScanAndIndex.FNV_1(w)

		if (!wordsHashMap.containsKey(hashkey)) {
			Index = Index + 1
			
			arrayOfPaths(Index) = new Array[String](5)

			wordsHashMap.put(hashkey, Index)

		} else {

			existeHashkey = true

			index_temp =  Index

			Index = wordsHashMap.get(hashkey)

			var c : Int = 0

			arrayOfPaths(Index).foreach { p =>
				if (p != null) {
					c = c + 1
				}
			}

			j = c

		}

		try {

			arrayOfPaths(Index)(j) = path_filename

		} catch {

			case e:java.lang.ArrayIndexOutOfBoundsException => {
				var in = new Indexer(fsScanner)
				
				fsScanner ! NewIndexer(in)

				in.start()

				in ! Word(w, path_filename)

				Main.indexers = in :: Main.indexers

			}

		}

		if (existeHashkey) {
			j = j + 1
			Index = index_temp
		}

	}


	def showPaths(str : String) {

		val hashkey : Long = ScanAndIndex.FNV_1(str)
		var Index = wordsHashMap.get(hashkey)

		if (Index != 0) {
			val len : Int = arrayOfPaths(Index).length
			for(j<-0 to len-1) {
				if (arrayOfPaths(Index)(j) != null) {
					println(arrayOfPaths(Index)(j))
				}
			}
		}

	}


	def act() {

		var conta : Int = 0

		loop {
			react {				
				case Word(w, path_filename) => {
					insertArrayOfPaths(w, path_filename)
				}
				case SearchIndexer(str : String) => {
					showPaths(str)
				}
				case Stop => {
					exit()
				}
			}
		}
	}

}


object Searcher extends Actor {

	var index : Int = 0

	def act() {

		println("[Searcher] starting...")

		loop {
			react {
				case Search => {
					var cycle : Boolean = true;
					while(cycle) {
						print("Please enter a string to search files: ")
						var str = Console.readLine
						if (str == "exit") {
							cycle = false
						} else if (str != null) {
							search(str)
						}
						Thread.sleep(1000)
					}
					sender ! Stop
					this ! Stop
				}
				case Stop => {
					println("[Searcher] stoping...")
					exit()
				}
			}
		}

	}

	def search(s : String) {
		Main.indexers.foreach{ i =>  
			i ! SearchIndexer(s) 
		}
	}

}


class MainActor extends Actor {

	def act() {

		var filter : FileFilter = new FileFilter {
			def  accept(file : File) : Boolean = return true;
		}

		val fScanner : FileScanner = new FileScanner(self)

		fScanner.start
		fScanner ! StartFileScanner

		val file1 : File = new File("/home/marcelo/")
		val fc = new FileCrawler(fScanner, filter, file1)
		fc.start
		fc ! Crawl

		loop {
			react {
				case Search => {
					Searcher.start
					Searcher ! Search
				}
				case Stop => {
					println("[MainActor] stopping...")
					System.exit(0);
				}
			}
		}

	}

}


object ScanAndIndex {

	def FNV_1(word : String) : Long = {

		val offset_basis : Long = 2166136261L;   // 32bit offset_basis = 2166136261
		val FNV_prime : Long = 16777619L;		 // 32bit FNV_prime = 16777619

		val wordByteArray : Array[Byte] = word.getBytes();
		var hash : Long = 0L;

		hash = offset_basis

		// for each octet_of_data to be hashed
		for( i <- 0 to wordByteArray.length-1 ) {
			hash = hash * FNV_prime
			// hash = hash xor octet_of_data
			hash = hash ^ wordByteArray(i)
		}

		//return hash
		hash
	}

	def startIndexing() {
		var m = new MainActor
		m.start()
	}

}