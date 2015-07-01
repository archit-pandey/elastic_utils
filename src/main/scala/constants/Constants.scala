package constants

import java.util.regex.Pattern

/**
 * Created by IntelliJ IDEA.
 * User: archit.pandey
 * Date: 6/30/2015
 * Time: 10:01 AM
 */
trait Constants extends AliasOperation {

  val aliasPrefix : String = "es_"
  val backupIndexPrefix: String = "backup_"
  val snapshotNamePattern: Pattern = Pattern.compile("snapshot_(\\d\\d\\d\\d)\\-(\\d\\d)\\-(\\d\\d)\\-(\\d\\d\\d\\d)")
}

object ConstantTest extends Constants with App {

   val matches = snapshotNamePattern.matcher("snapshot_2015-06-30-0000").matches()
    println("Matches: " + matches)
}
