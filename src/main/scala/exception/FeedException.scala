package exception

class FeedException(sourceName:String) extends RuntimeException(s"The validation for ${sourceName} failed.")
