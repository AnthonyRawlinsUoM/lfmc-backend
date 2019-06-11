from swiftclient import client, exceptions


class SwiftStorage:

    def __init__(self):
        # self.url = url
        # self.username = usernmae
        # self.password = password
        # self.project_name = project
        # self.container_name = container
        self.url = 'https://keystone.rc.nectar.org.au:5000/v3/'
        self.username = 'anthony.rawlins@unimelb.edu.au'
        self.password = 'MDI3NjkwMzcwMjZjYmQz'
        self.project_name = 'LFMC'
        self.container_name = 'MODIS'
        self.swift = client.Connection(authurl=self.url, user=self.username, key=self.password,
                                       tenant_name=self.project_name, auth_version='3')

    def swift_put_modis(self, file_name):
        container_name = 'MODIS'
        return self.put(container_name, file_name)

    def swift_check_modis(self, object_name):
        success = False
        try:
            resp_headers = self.swift.head_object('MODIS', object_name)
            logger.debug("%s exists." % object_name)
            success = True
        except exceptions.ClientException as e:
            if e.http_status == '404':
                logger.debug("The object: %s was not found." % object_name)
            else:
                logger.debug(
                    "An error occurred checking the existence of object: %s" % object_name)
        return success

    def swift_get_modis(self, object_name):
        if self.swift_check_modis(object_name):
            resp_headers, obj_contents = self.swift.get_object(
                'MODIS', object_name)
            with open(object_name, 'w+b') as so:
                so.write(obj_contents)
            return True
        else:
            return False

    def swift_put_lfmc(self, file_name):
        container_name = 'lfmc'
        return self.put(container_name, file_name)

    def swift_check_lfmc(self, file_name):
        success = False
        try:
            logger.debug('Checking SwiftStorage for: %s' % file_name)
            resp_headers = self.swift.head_object('lfmc', str(file_name))

            logger.debug("%s exists." % file_name)
            success = True
        except TypeError as te:
            logger.debug('Problem parsing file name?? ', file_name)
        except exceptions.ClientException as e:
            if e.http_status == '404':
                logger.debug("The object: %s was not found." % file_name)
            else:
                logger.debug(
                    "An error occurred checking the existence of object: %s" % file_name)
        return success

    def swift_get_lfmc(self, object_name):
        if self.swift_check_modis(object_name):
            resp_headers, obj_contents = self.swift.get_object(
                'lfmc', object_name)
            with open(object_name, 'w+b') as so:
                so.write(obj_contents)
            return True
        else:
            return False

    def put(self, container_name, file_name):
        with open(file_name, 'r+b') as local:
            self.swift.put_object(container_name, file_name,
                                  contents=local, content_type='application/binary')
        return self.swift_check_modis(file_name)


# test = SwiftStorage()

# assert not test.swift_check_lfmc('anything.txt')
