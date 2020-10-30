package net.pucgenie.diskcache.force;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 *
 * @author pucgenie
 */
public class ReadSmallRecursively {

	private static final java.util.ResourceBundle BUNDLE = java.util.ResourceBundle
			.getBundle("net/pucgenie/diskcache/force/ReadSmallRecursively");
	private static final java.util.logging.Logger LOG = java.util.logging.Logger
			.getLogger(ReadSmallRecursively.class.getName());

	private final ArrayList<File> smallFiles;

	/**
	 * Wie viele Dateien sich (in etwa) in jedem bestimmten Verzeichnis befinden.
	 */
	private Properties stats;

	private int geladen;

	private boolean kaputt = false;

	private Pattern filterIncl, filterExcl;

	public ReadSmallRecursively(Optional<File> stF) {
		int minKapazität = 0x1FFF;
		stF.ifPresent(statsFile -> {
			// mit der initial capacity so hoch wie der Anzahl der Dateien beim letzten
			// Durchlauf initialisieren.
			this.stats = new Properties();
			if (statsFile.exists() && statsFile.canRead()) {
				try (var fis = new FileInputStream(statsFile);
						var sreader = new InputStreamReader(fis, StandardCharsets.UTF_8)) {
					this.stats.load(fis);
				} catch (IOException ex) {
					LOG.log(Level.SEVERE, BUNDLE.getString("KONNTE STATS-DATEI NICHT LADEN."), ex);
				}
			}
			// eigentlicher Check wird in #link{#scan()} durchgeführt
		});
		smallFiles = new ArrayList<>(minKapazität);
	}

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
	 * Durchsucht den angegebenen Ordner und eventuell Subordner bis zu
	 * <code>rekursiv</code> Ebenen.;
	 *
	 * @param basisordner
	 * @param rekursiv
	 */
	public void scan(File basisordner, short rekursiv) {
		String fstStr = stats != null ? stats.getProperty(basisordner.getAbsolutePath()) : null;
		int oldSize = smallFiles.size();
		if (fstStr != null && fstStr.isEmpty() == false) {
			smallFiles.ensureCapacity(oldSize + Integer.parseInt(fstStr));
		}
		try {
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
				if (sub.isDirectory() && rekursiv > 0) {
					scan(sub, (short) (rekursiv - 1));
					if (kaputt) {
			return;
					}
				} else {
					smallFiles.add(sub);
				}
			}
		} catch (OutOfMemoryError oomERROR) {
			oomERROR = null;
			stats = null;
			// GarbageCollector soll machen
			kaputt = true;
			smallFiles.clear();
			// nochmal GC !
			smallFiles.trimToSize();
			// nochmal GC ...
			return;
		}
		if (fstStr != null && oldSize != smallFiles.size()) {
			assert smallFiles.size() > oldSize : "wtf?"; // NOI18N
			stats.setProperty(basisordner.getAbsolutePath(), Integer.toString(smallFiles.size() - oldSize));
		}
	}

	/**
	 * Öffnet alle gescannten Dateien. Wenn deep false ist, liefert diese Methode
	 * auf jeden Fall false.
	 *
	 * @param limitN
	 * @param limitRead Anzahl von 4K-Blöcken, die "gelesen" werden sollen
	 * @param deep      Durchsucht weitere Verzeichnisse, die beim Scannen zu tief
	 *                  rekursiv waren.
	 * @return deep && mehr Unterebenen gefunden.
	 * @throws IOException
	 */
	public boolean load(int limitN, int limitRead, boolean deep) throws IOException {
		var lastIdx = Math.min(smallFiles.size(), limitN);
		LOG.log(Level.FINER, "lade {0}", lastIdx);
		final var deeperList = new ArrayList<File>(10);
		final var workList = smallFiles.subList(0, lastIdx);
		final var liter = workList.listIterator(0);
		dateiSchleife: while (liter.hasNext()) {
			final File f = liter.next();
			if (f.isDirectory()) {
				deeperList.add(f);
		continue;
			}
			if (f.canRead() == false) {
				// wird momentan ignoriert. was bringt's...
		continue;
			}
			try (FileInputStream ffif = new FileInputStream(f)) {
				++geladen;
				for (int nx = limitRead; nx > 0; --nx) {
					// zum Ende eines 4K-Blobks springen und ein Byte lesen (und verwerfen)
					ffif.skip(4095);
					if (ffif.read() == -1) {
						// nx = 0;
						continue dateiSchleife;
					}
				}
			} catch (FileNotFoundException ex) {
				assert false : ex;
			}
		}
		workList.clear();
		if (deep) {
			// nicht parallelisieren!
			for (var f : deeperList) {
				scan(f, (short) 0);
			}
		} else {
			smallFiles.addAll(deeperList);
		}
		// deeper.clear();
		// deeper = null;
		return smallFiles.isEmpty() == false;
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
		try (var stos = new FileOutputStream(stF)) {
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
	public static void main(String[] args) throws IOException {
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

		var basedir = Optional.ofNullable(System.getProperty("rsr.basedir")).map(basedirStr -> new File(basedirStr)).orElse(new File(".")); // NOI18N
		var recursions = Optional.ofNullable(System.getProperty("rsr.recursions")).map(Short::parseShort).orElse((short) 4); // NOI18N
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
				"(_.*)|(.*\\.bak)|(.*\\.dll)|(.*\\.csb)|(.*\\.rpack)"); // NOI18N

		int filterCaseFlag = filterCase.equals("ci") ? Pattern.CASE_INSENSITIVE : 0;

		while (upwards --> 0) {
			basedir = basedir.getParentFile();
		}
		if (basedir.isDirectory() == false) {
			throw new IllegalArgumentException(BUNDLE.getString("RSR.BASEDIR MUSS EIN VERZEICHNIS SEIN"));
		}
		LOG.log(Level.FINE, "scanne {0}", basedir.getAbsolutePath());

		final var statsFile = Optional.ofNullable(System.getProperty("rsr.stats")).map(statsFileStr -> new File(statsFileStr));

		final var rsr = new ReadSmallRecursively(statsFile);
		filterInclProp.ifPresent(filterInclStr -> {
			rsr.filterIncl = Pattern.compile(filterInclStr, filterCaseFlag);
		});

		if (!filterExclStr.isEmpty()) {
			rsr.filterExcl = Pattern.compile(filterExclStr, filterCaseFlag);
		}
		rsr.scan(basedir, recursions);

		boolean logIt = Level.FINER.equals(LOG.getLevel());
		String[] lmo = { "~", BUNDLE.getString("EINE EBENE MEHR WIRD GELADEN.")};
		while (!rsr.kaputt && rsr.load(0x2FF, 0xF00, true)) {
			if (logIt) {
				lmo[0] = Integer.toString(rsr.geladen);
				LOG.log(Level.FINER, "{0}\t{1}", lmo);
			}
		}
		if (rsr.kaputt) {
			LOG.log(Level.SEVERE,
					"OutOfMemory Fehler - irgendwie überstanden, aber Einlesen war vermutlich unvollständig");
		}
		if (logIt) {
			lmo[0] = Integer.toString(rsr.geladen);
			lmo[1] = BUNDLE.getString("FERTIG.");
			LOG.log(Level.FINE, "{0}\t{1}", lmo);
		}

		{
			final var measurement = rsr.measurement;
			Collections.sort(measurement);
			LOG.log(Level.FINEST, "Min filter time[ns]: {0}", measurement.get(0));
			Number median = measurement.get(measurement.size() / 2);
			if (measurement.size() % 2 == 0) {
				median = (median.longValue() + measurement.get(measurement.size() / 2 + 1)) / 2.;
			}
			LOG.log(Level.FINEST, "{0}ns Median filter time, {1}ns max. filter time", new Object[] {median, measurement.get(measurement.size() - 1)});
			long sum = 0;
			for (long msw : measurement) {
				sum += msw;
			}
			LOG.log(Level.FINEST, "{0}ms cumul. filter time", sum / 1_000_000.);
		}

		statsFile.ifPresent(rsr::syncStats);
	}

}
