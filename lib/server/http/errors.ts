export class HttpNotFoundError extends Error {}
export class HttpBadRequestError extends Error {}

export const httpErrorTypes = {
    notFound: HttpNotFoundError,
    badRequest: HttpBadRequestError
};
