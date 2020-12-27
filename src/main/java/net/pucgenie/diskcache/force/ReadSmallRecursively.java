package net.pucgenie.diskcache.force;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 *
 * @author pucgenie
 */
public class ReadSmallRecursively implements Runnable {

	private static final java.util.ResourceBundle BUNDLE = java.util.ResourceBundle
			.getBundle(ReadSmallRecursively.class.getCanonicalName().replace('.', '/'));
	private static final java.util.logging.Logger LOG = java.util.logging.Logger
			.getLogger(ReadSmallRecursively.class.getName());
	static {
		if (LOG.getHandlers().length == 0) {
			final Handler handler = new ConsoleHandler();// new StreamHandler(System.out, new SimpleFormatter());
			handler.setLevel(Level.FINEST);
			LOG.addHandler(handler);
		}
		if (LOG.getLevel() == null) {
			LOG.setLevel(Level.FINEST);
		}
	}

	private final ArrayBlockingQueue<File> smallFiles = new ArrayBlockingQueue<>(0xFF);
	private final ArrayList<File> dirsToScan = new ArrayList<>(0xFF);

	private Thread cacheFiller = new Thread(null, this, "CacheFiller", 4);
	{
		cacheFiller.setDaemon(true);
	}
	private volatile boolean listingFinished = false;

	/**
	 * Wie viele Dateien sich (in etwa) in jedem bestimmten Verzeichnis befinden.
	 */
	private Properties stats;

	private int geladen;

	/**
	 * Anzahl von 512B-Blöcken, die "gelesen" werden sollen.
	 */
	private int limitRead = 0x4;

	private Pattern filterIncl, filterExcl;

	public ReadSmallRecursively(Optional<File> stF) {
		stF.ifPresent(statsFile -> {
			// mit der initial capacity so hoch wie der Anzahl der Dateien beim letzten
			// Durchlauf initialisieren.
			this.stats = new Properties();
			if (statsFile.exists() && statsFile.canRead()) {
				try (var fis = new FileReader(statsFile, StandardCharsets.UTF_8)) {
					this.stats.load(fis);
				} catch (IOException ex) {
					LOG.log(Level.SEVERE, BUNDLE.getString("KONNTE STATS-DATEI NICHT LADEN."), ex);
				}
			}
			// eigentlicher Check wird in #link{#scan()} durchgeführt
		});
	}

	@Override
	public void run() {
		try {
			dateiSchleife: while (!listingFinished || !smallFiles.isEmpty()) {
				var aFile = smallFiles.poll(5, TimeUnit.SECONDS);
				if (aFile == null) {
					LOG.warning("Traverser is slow.");
					continue;
				}
				if (aFile.canRead() == false) {
					// wird momentan ignoriert. was bringt's...
					continue dateiSchleife;
				}
				try (var ffif = new FileInputStream(aFile)) {
					++geladen;
					for (int nx = limitRead; nx > 0; --nx) {
						// zum Ende eines Blocks springen und ein Byte lesen (und verwerfen)
						// if it skips zero bytes once, it doesn't really matter.
						ffif.skip(511);
						if (ffif.read() == -1) {
							// nx = 0;
							continue dateiSchleife;
						}
					}
				} catch (FileNotFoundException ex) {
					LOG.log(Level.SEVERE, "File vanished before being opened: {0}", aFile.getPath());
				} catch (IOException ex) {
					LOG.log(Level.SEVERE, "Couldn't read: {0}", aFile.getPath());
				}
			}
		} catch (InterruptedException e) {
			listingFinished = true;
			LOG.log(Level.SEVERE, "", e);
		}
		LOG.log(Level.FINEST, "CacheFiller finished.");
	}

	/**
	 * Entfernt alle gescannten Dateien aus der Liste der zu ladenden Dateien.
	 * Sollte im Normalfall nicht notwendig sein. Die backing-List wird
	 * <strong>nicht</strong> verkleinert.
	 */
	public void clear() {
		smallFiles.clear();
		geladen = 0;
	}

	/**
	 * Autoboxing happens
	 */
	private final List<Long> measurement = new ArrayList<>(0xFFFF);

	/**
	 * Durchsucht den angegebenen Ordner und eventuell Unterordner bis zu
	 * <code>rekursiv</code> Ebenen.
	 *
	 * @param basisordner
	 * @param rekursiv
	 * @throws InterruptedException 
	 */
	public void scan(File basisordner, short rekursiv) throws InterruptedException {
		for (var subFile : basisordner.list()) {
			final var startTime = System.nanoTime();
			if (filterIncl == null || filterIncl.matcher(subFile).matches()) {
				if (filterExcl != null && filterExcl.matcher(subFile).matches()) {
					measurement.add(System.nanoTime() - startTime);
		continue;
				}
			} else {
				measurement.add(System.nanoTime() - startTime);
		continue;
			}
			measurement.add(System.nanoTime() - startTime);

			final var sub = new File(basisordner, subFile);
			if (sub.isDirectory()) {
				if (rekursiv > 0) {
					dirsToScan.add(sub);
				}
			} else {
				while (!smallFiles.offer(sub, 10, TimeUnit.SECONDS)) {
					LOG.log(Level.WARNING, "Worker is slow, waiting...");
				}
			}
		}
	}
	
	public void scanLoop(File basedir, short recursions) throws InterruptedException {
		dirsToScan.add(basedir);
		do {
			// don't get confused about newly added things in dirsToScan
			for (int i = dirsToScan.size(); i --> 0; ) {
				scan(dirsToScan.remove(i), recursions);
			}
		} while (!dirsToScan.isEmpty() && recursions --> 0);
		LOG.log(Level.FINEST, "Traverser finished.");
		this.listingFinished = true;
	}

	/**
	 *
	 * @param stF
	 * @throws IOException
	 */
	public void saveStats(File stF) throws IOException {
		if (stats == null) {
			throw new IllegalStateException(BUNDLE.getString("KEINE STATS GELADEN"));
		}
		LOG.log(Level.FINE, "Storing stats in {0}", stF.getAbsolutePath());
		try (var stos = new FileWriter(stF, StandardCharsets.UTF_8)) {
			stats.store(stos, BUNDLE.getString("GENERATED STATS"));
		}
	}

	public boolean hasStats() {
		return stats != null;
	}

	public void syncStats(File stF) throws UncheckedIOException {
		if (!hasStats()) {
			return;
		}
		try {
			saveStats(stF);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * Unerwartete Fehlschläge werden auch mitgezählt. Unerwartet: file.canRead()
	 * ist true, aber FileInputStream kann es trotzdem nicht (korrekt) lesen.
	 *
	 * @return wie viele Dateien geladen wurden.
	 */
	public int getSeenCount() {
		return geladen;
	}

	/**
	 * @param args the command line arguments
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length > 0) {
			switch (args[0]) {
			case "/?": // NOI18N
			case "/h": // NOI18N
			case "-?": // NOI18N
			case "-h": // NOI18N
			case "--help": // NOI18N
				LOG.log(Level.CONFIG, BUNDLE.getString("VERWENDUNG"));
				return;
			default:
				LOG.log(Level.SEVERE, BUNDLE.getString("UNGÜLTIGES ARGUMENT."));
			}
		}

		var basedir = Optional.ofNullable(System.getProperty("rsr.basedir")).map(basedirStr -> new File(basedirStr))
				.orElse(new File(".")); // NOI18N
		short recursions = Optional.ofNullable(System.getProperty("rsr.recursions")).map(Short::parseShort)
				.orElse((short) 4); // NOI18N
		var upwards = Optional.ofNullable(System.getProperty("rsr.upwards")).map(Byte::parseByte).orElse((byte) 0); // NOI18N

		/**
		 * case kann sein: cs (default): case sensitive ci: case insensitive
		 */
		String filterCase = System.getProperty("rsr.filterCase", "cs"); // NOI18N
		var filterInclProp = Optional.ofNullable(System.getProperty("rsr.filterInclude")); // NOI18N
		/**
		 * _* common redist installer files *.bak unimportant files *.dll do not cache
		 * library file data *.csb Dying Light big files *.rpack Dying Light big files
		 */
		var filterExclStr = System.getProperty("rsr.filterExclude",
				"(_.*)|(.*\\.bak)|(.*\\.csb)|(.*\\.rpack)"); // NOI18N

		int filterCaseFlag = filterCase.equals("ci") ? Pattern.CASE_INSENSITIVE : 0;

		while (upwards --> 0) {
			basedir = basedir.getParentFile();
		}
		if (basedir.isDirectory() == false) {
			throw new IllegalArgumentException(BUNDLE.getString("RSR.BASEDIR MUSS EIN VERZEICHNIS SEIN"));
		}
		LOG.log(Level.FINE, "scanne {0}", basedir.getAbsolutePath());

		final var statsFile = Optional.ofNullable(System.getProperty("rsr.stats"))
				.map(statsFileStr -> new File(statsFileStr));

		final var rsr = new ReadSmallRecursively(statsFile);
		filterInclProp.ifPresent(filterInclStr -> {
			rsr.filterIncl = Pattern.compile(filterInclStr, filterCaseFlag);
		});

		if (!filterExclStr.isEmpty()) {
			rsr.filterExcl = Pattern.compile(filterExclStr, filterCaseFlag);
		}
		rsr.cacheFiller.start();
		rsr.scanLoop(basedir, recursions);
		rsr.cacheFiller.join(0);
		assert rsr.smallFiles.isEmpty();

		LOG.log(Level.FINE, "{0}\t{1}", new Object[] {Integer.toString(rsr.geladen), BUNDLE.getString("FERTIG."),});

		{
			final var measurement = rsr.measurement;
			Collections.sort(measurement);
			Number median = measurement.get(measurement.size() / 2);
			if (measurement.size() % 2 == 0) {
				median = (median.longValue() + measurement.get(measurement.size() / 2 + 1)) / 2.;
			}
			long sum = 0;
			for (long msw : measurement) {
				sum += msw;
			}
			LOG.log(Level.FINE,
					"{3}ms cumul. filter time, {2}ns min. filter time, {0}ns Median filter time, {1}ns max. filter time",
					new Object[] { median, measurement.get(measurement.size() - 1), measurement.get(0),
							sum / 1_000_000., });
		}

		statsFile.ifPresent(rsr::syncStats);
	}

}
