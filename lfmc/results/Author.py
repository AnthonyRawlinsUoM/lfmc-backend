from marshmallow import Schema, fields


class Author:
  def __init__(self, name, email, organisation):
    """Short summary.

    Parameters
    ----------
    name : type
        Description of parameter `name`.
    email : type
        Description of parameter `email`.
    organisation : type
        Description of parameter `organisation`.

    Returns
    -------
    type
        Description of returned object.

    """
    self.name = name
    self.email = email
    self.organisation = organisation


class AuthorSchema(Schema):
  name = fields.String()
  email = fields.Email()
  organisation = fields.String()
