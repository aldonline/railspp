package api

object Api {

  def createApp: App = null

  def insertFacebookToken( app: App, id: ExternalId, ft: FacebookTokens ) {

  }

  def getFriends = {

  }

  def connectRailsApp( dsn: DSN ) {

  }
}

case class ExternalId
case class FacebookTokens
case class FacebookAccessToken
case class FacebookRefreshToken
case class DSN // database DSN
trait App
trait Domain
trait RailsApp {
  def dsn: DSN
}