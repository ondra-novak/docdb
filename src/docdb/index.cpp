#include "index.h"

#include "storage.h"



namespace docdb {

Index::Index(PDatabase db, std::string_view name, Direction dir, PSnapshot snap)
        :View(std::move(db), name, dir, std::move(snap))
{
    _subindex = 0;
}

Index::Index(PDatabase db, KeyspaceID kid, Direction dir, PSnapshot snap)
    :View(std::move(db), kid, dir, std::move(snap))
{
    _subindex = 0;
}

}

