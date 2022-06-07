package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/gobwas/glob"
	"github.com/redhat-et/fetchit/pkg/engine/utils"
)

func (fc *FetchitConfig) CatchUpCurrent(ctx context.Context, mo *SingleMethodObj, current plumbing.Hash, targetPath string, tag *[]string, globPattern *string) error {
	err := fc.Apply(ctx, mo, zeroHash, current, targetPath, tag, globPattern)
	if err != nil {
		return utils.WrapErr(err, "Failed to apply changes")
	}

	return nil
}

func (fc *FetchitConfig) CatchUpProgress(ctx context.Context, mo *SingleMethodObj, current, progress plumbing.Hash, targetPath string, tag *[]string, globPattern *string) error {
	if progress != current {
		err := fc.Apply(ctx, mo, current, progress, targetPath, tag, globPattern)
		if err != nil {
			if err := fc.DeleteInProgress(ctx, mo.Target, mo.Method); err != nil {
				return utils.WrapErr(err, "Failed to delete progress")
			}

			return utils.WrapErr(err, "Failed to apply from current to in progress")
		}

		err = fc.UpdateCurrent(ctx, mo.Target, mo.Method, progress)
		if err != nil {
			return utils.WrapErr(err, "Failed to update current to progress")
		}
	}

	if err := fc.DeleteInProgress(ctx, mo.Target, mo.Method); err != nil {
		return utils.WrapErr(err, "Failed to delete progress")
	}

	return nil
}

func (fc *FetchitConfig) CatchUpLatest(ctx context.Context, mo *SingleMethodObj, current, latest plumbing.Hash, targetPath string, tag *[]string, globPattern *string) error {
	err := fc.CreateInProgress(ctx, mo.Target, mo.Method, latest)
	if err != nil {
		return utils.WrapErr(err, "Failed to create progress tag")
	}

	err = fc.Apply(ctx, mo, current, latest, targetPath, tag, globPattern)
	if err != nil {
		if err := fc.DeleteInProgress(ctx, mo.Target, mo.Method); err != nil {
			return utils.WrapErr(err, "Error deleting progress tag")
		}

		return utils.WrapErr(err, "Failed to apply changes")
	}

	err = fc.UpdateCurrent(ctx, mo.Target, mo.Method, latest)
	if err != nil {
		return utils.WrapErr(err, "Error updating current tag")
	}

	err = fc.DeleteInProgress(ctx, mo.Target, mo.Method)
	if err != nil {
		return utils.WrapErr(err, "Error deleting progress tag")
	}

	return nil
}

/*
For any given target, will get the head of the branch
in the repository specified by the target's url
*/
func (fc *FetchitConfig) GetLatest(target *Target, method string) (plumbing.Hash, error) {
	directory := filepath.Base(target.Url)

	repo, err := git.PlainOpen(directory)
	if err != nil {
		return plumbing.Hash{}, utils.WrapErr(err, "Error opening repository: %s", directory)
	}

	refSpec := config.RefSpec(fmt.Sprintf("+refs/heads/%s:refs/heads/%s", target.Branch, target.Branch))
	if err = repo.Fetch(&git.FetchOptions{
		RefSpecs: []config.RefSpec{refSpec, "HEAD:refs/heads/HEAD"},
		Force:    true,
	}); err != nil && err != git.NoErrAlreadyUpToDate {
		return plumbing.Hash{}, utils.WrapErr(err, "Error fetching branch %s from remote repository %s", target.Branch, target.Url)
	}

	branch, err := repo.Reference(plumbing.ReferenceName(fmt.Sprintf("refs/heads/%s", target.Branch)), false)
	if err != nil {
		return plumbing.Hash{}, utils.WrapErr(err, "Error getting reference to branch %s", target.Branch)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return plumbing.Hash{}, utils.WrapErr(err, "Error getting reference to worktree for repository", target.Name)
	}

	err = wt.Checkout(&git.CheckoutOptions{Hash: branch.Hash()})
	if err != nil {
		return plumbing.Hash{}, utils.WrapErr(err, "Error checking out %s on branch %s", branch.Hash(), target.Branch)
	}

	return branch.Hash(), err
}

func (fc *FetchitConfig) GetCurrent(target *Target, method string) (plumbing.Hash, error) {
	directory := filepath.Base(target.Url)
	tagName := fmt.Sprintf("current-%s", method)

	repo, err := git.PlainOpen(directory)
	if err != nil {
		return plumbing.Hash{}, utils.WrapErr(err, "Error opening repository: %s", directory)
	}

	ref, err := repo.Tag(tagName)
	if err == git.ErrTagNotFound {
		return plumbing.Hash{}, nil
	} else if err != nil {
		return plumbing.Hash{}, utils.WrapErr(err, "Error getting reference to current tag")
	}

	return ref.Hash(), err
}

func (fc *FetchitConfig) UpdateCurrent(ctx context.Context, target *Target, method string, newCurrent plumbing.Hash) error {
	directory := filepath.Base(target.Url)
	tagName := fmt.Sprintf("current-%s", method)

	repo, err := git.PlainOpen(directory)
	if err != nil {
		return utils.WrapErr(err, "Error opening repository: %s", directory)
	}

	err = repo.DeleteTag(tagName)
	if err != nil && err != git.ErrTagNotFound {
		return utils.WrapErr(err, "Error deleting old current tag")
	}

	_, err = repo.CreateTag(tagName, newCurrent, nil)
	if err != nil {
		return utils.WrapErr(err, "Error creating new current tag with hash %s", newCurrent)
	}

	return nil
}

func (fc *FetchitConfig) GetInProgress(ctx context.Context, target *Target, method string) (plumbing.Hash, error) {
	directory := filepath.Base(target.Url)
	tagName := fmt.Sprintf("progress-%s", method)

	repo, err := git.PlainOpen(directory)
	if err != nil {
		return plumbing.Hash{}, utils.WrapErr(err, "Error opening repository: %s", directory)
	}

	ref, err := repo.Tag(tagName)
	if err != nil {
		if err == git.ErrTagNotFound {
			return plumbing.Hash{}, nil
		}
		return plumbing.Hash{}, utils.WrapErr(err, "Error getting in progress tag")
	}

	return ref.Hash(), nil
}

func (fc *FetchitConfig) CreateInProgress(ctx context.Context, target *Target, method string, latest plumbing.Hash) error {
	directory := filepath.Base(target.Url)
	tagName := fmt.Sprintf("progress-%s", method)

	repo, err := git.PlainOpen(directory)
	if err != nil {
		return utils.WrapErr(err, "Error opening repository: %s", directory)
	}

	_, err = repo.CreateTag(tagName, latest, nil)
	if err != nil && err != git.ErrTagExists {
		return utils.WrapErr(err, "Error creating progress tag with hash %s", latest)
	}

	return nil
}

func (fc *FetchitConfig) DeleteInProgress(ctx context.Context, target *Target, method string) error {
	directory := filepath.Base(target.Url)
	tagName := fmt.Sprintf("progress-%s", method)

	repo, err := git.PlainOpen(directory)
	if err != nil {
		return utils.WrapErr(err, "Error opening repository: %s", directory)
	}

	err = repo.DeleteTag(tagName)
	if err != nil && err != git.ErrTagNotFound {
		return utils.WrapErr(err, "Error deleting progress tag")
	}

	return nil
}

// Side effects are running/applying changes concurrently and on success moving old "current" tag
func (fc *FetchitConfig) Apply(
	ctx context.Context,
	mo *SingleMethodObj,
	currentState plumbing.Hash,
	desiredState plumbing.Hash,
	targetPath string,
	tags *[]string,
	globPattern *string,
) error {
	if desiredState.IsZero() {
		return errors.New("Cannot run Apply if desired state is empty")
	}
	directory := filepath.Base(mo.Target.Url)

	currentTree, err := getSubTreeFromHash(directory, currentState, targetPath)
	if err != nil {
		return utils.WrapErr(err, "Error getting sub tree from hash %s from repo %s", currentTree, directory)
	}

	desiredTree, err := getSubTreeFromHash(directory, desiredState, targetPath)
	if err != nil {
		return utils.WrapErr(err, "Error getting sub tree from hash %s from repo %s", desiredState, directory)
	}

	changeMap, err := getFilteredChangeMap(directory, targetPath, currentTree, desiredTree, tags, globPattern)
	if err != nil {
		return utils.WrapErr(err, "Error getting filtered change map from %s to %s", currentState, desiredState)
	}

	err = fc.runChangesConcurrent(ctx, mo, changeMap)
	if err != nil {
		return utils.WrapErr(err, "Error applying change from %s to %s for path %s in %s",
			currentState, desiredState, targetPath, directory,
		)
	}

	return nil
}

func getSubTreeFromHash(directory string, hash plumbing.Hash, subDir string) (*object.Tree, error) {
	if hash.IsZero() {
		return &object.Tree{}, nil
	}

	repo, err := git.PlainOpen(directory)
	if err != nil {
		return nil, utils.WrapErr(err, "Error opening repository: %s", directory)
	}

	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, utils.WrapErr(err, "Error getting commit at hash %s from repo %s", hash, directory)
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, utils.WrapErr(err, "Error getting tree from commit at hash %s from repo %s", hash, directory)
	}

	subDirTree, err := tree.Tree(subDir)
	if err != nil {
		return nil, utils.WrapErr(err, "Error getting sub tree at path %s", subDir)
	}

	return subDirTree, nil
}

func getFilteredChangeMap(
	directory string,
	targetPath string,
	currentTree,
	desiredTree *object.Tree,
	tags *[]string,
	globPattern *string,
) (map[*object.Change]string, error) {
	var g glob.Glob
	var err error
	if globPattern == nil {
		g, err = glob.Compile("**")
		if err != nil {
			return nil, utils.WrapErr(err, "Error compiling glob for pattern %s", globPattern)
		}
	} else {
		g, err = glob.Compile(*globPattern)
		if err != nil {
			return nil, utils.WrapErr(err, "Error compiling glob for pattern %s", globPattern)
		}
	}

	changes, err := currentTree.Diff(desiredTree)
	if err != nil {
		return nil, utils.WrapErr(err, "Error getting diff between current and latest", targetPath)
	}

	changeMap := make(map[*object.Change]string)
	for _, change := range changes {
		if change.To.Name != "" && checkTag(tags, change.To.Name) && g.Match(change.To.Name) {
			path := filepath.Join(directory, targetPath, change.To.Name)
			changeMap[change] = path
		} else if change.From.Name != "" && checkTag(tags, change.From.Name) && g.Match(change.To.Name) {
			checkTag(tags, change.From.Name)
			changeMap[change] = deleteFile
		}
	}

	return changeMap, nil
}

func checkTag(tags *[]string, name string) bool {
	if tags == nil {
		return true
	}
	for _, suffix := range *tags {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func (fc *FetchitConfig) runChangesConcurrent(ctx context.Context, mo *SingleMethodObj, changeMap map[*object.Change]string) error {
	ch := make(chan error)
	for change, changePath := range changeMap {
		go func(ch chan<- error, changePath string, change *object.Change) {
			if err := fc.EngineMethod(ctx, mo, changePath, change); err != nil {
				ch <- utils.WrapErr(err, "error running engine method for change from: %s to %s", change.From.Name, change.To.Name)
			}
			ch <- nil
		}(ch, changePath, change)
	}
	for range changeMap {
		err := <-ch
		if err != nil {
			return err
		}
	}
	return nil
}
