package constants

object SnapShot extends Constants {
  def apply(snapshotName: String): SnapShot =
  {
    val matcher = snapshotNamePattern.matcher(snapshotName)
    matcher.matches()
    val year = Integer.parseInt(matcher.group(1))
    val month = Integer.parseInt(matcher.group(2))
    val date = Integer.parseInt(matcher.group(3))
    val slice = Integer.parseInt(matcher.group(4))
    new SnapShot(year, month, date, slice)
  }

}

class SnapShot (
                 year: Int,
                 month: Int,
                 date: Int,
                 slice: Int) extends Constants {

  private def separator = "-"

  def after(that : SnapShot) : Boolean = {
    if(that == null) return true
    if(that.getSnapshotYear > getSnapshotYear) return false
    if(that.getSnapshotMonth > getSnapshotMonth) return false
    if(that.getSnapshotDate > getSnapshotDate) return false
    if(that.getSnapshotSlice > getSnapshotSlice) return false
    true
  }

  def before(that:SnapShot) : Boolean = {
      !after(that)  //what if snapshot are same??
  }

  override def toString: String =
  {
    val builder = new StringBuilder()
    builder.append(year).append(separator)
      .append(month).append(separator)
      .append(date).append(separator)
      .append("00").append(if (slice < 10)
    {
      "0"
    } else
    {
      ""
    }).append(slice)

    builder.toString()
  }

  def getSnapshotDate : Int = date

  def getSnapshotYear : Int = year

  def getSnapshotMonth : Int = month

  def getSnapshotSlice : Int = slice
}
